using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using FastMember;

namespace Dapper.FastBulkOperations.SqlServer;

public static partial class BulkExtensions
{
    private const int MemberSetCacheLimit = 10000;

    private record Identity(string ColumnName, string Type);

    private record struct CacheItem(
        TypeAccessor TypeAccessor, string TableName, string Schema, Dictionary<string, Member> ColumnsToProperty, List<string> PrimaryKeys, string[] PropertyNames)
    {
        public IEnumerable<string> ColumnNames => ColumnsToProperty.Keys;

        public volatile int HitPoints = 0;
    }

    private static volatile int _collected;
    private static readonly ConcurrentDictionary<Type, CacheItem> MemberSetCache = new ();

    private static string GetCreateTempTableQuery(string tempTableName, string destination, IEnumerable<string> columnNames = null) => $"SELECT {(columnNames is null ? "*" : string.Join(',', columnNames))} INTO {tempTableName} FROM {destination} WITH(READUNCOMMITTED) WHERE 1 = 0";
    
    private static CacheItem GetTypeCacheItem<T>()
    {
        if (MemberSetCache.TryGetValue(typeof(T), out var cacheItem))
        {
            
            Interlocked.Increment(ref cacheItem.HitPoints);
            return cacheItem;
        }
        if (Interlocked.Increment(ref _collected) >= MemberSetCacheLimit)
        {
            foreach (var item in MemberSetCache)
            {
                var temp = item.Value.HitPoints;
                if (Interlocked.CompareExchange(ref temp, 0, 0) <= 0)
                {
                    MemberSetCache.TryRemove(item);
                }
            }
        }

        var typeAccessor = TypeAccessor.Create(typeof(T));
        var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>(true);
        var memberSet = typeAccessor.GetMembers().Where(x => x.GetAttribute(typeof(NotMappedAttribute), true) is null).ToList();
        var primaryKeys = memberSet.Where(x => x.GetAttribute(typeof(KeyAttribute), true) is not null).Select(x => 
            x.GetAttribute(typeof(ColumnAttribute), true) is ColumnAttribute column ? column?.Name : x.Name).ToList();
        var columnsToProperties = memberSet.ToDictionary(x =>
            x.GetAttribute(typeof(ColumnAttribute), true) is ColumnAttribute column ? column?.Name : x.Name);
        cacheItem = new CacheItem
        {
            Schema = tableAttribute?.Schema ?? "dbo", 
            TableName = tableAttribute?.Name ?? typeof(T).Name, 
            TypeAccessor = typeAccessor, 
            ColumnsToProperty = columnsToProperties,
            PrimaryKeys = primaryKeys.Count is 0 ? null : primaryKeys,
            PropertyNames = columnsToProperties.Select(x => x.Value.Name).ToArray()
        };
        MemberSetCache.TryAdd(typeof(T), cacheItem);
        return cacheItem;
    }

    private static object Convert(object val)
    {
        if (val == null) return null;
        if (val is int i)
            val = i;
        if (val is uint ui)
            val = ui;
        if (val is short s)
            val = s;
        if (val is ushort us)
            val = us;
        if (val is long l)
            val = l;
        if (val is ulong ul)
            return ul;
        return val;
    }
    private static IList<T> MapIdentity<T>(IList<T> items, IDataReader reader, Identity identity)
    {
        if (identity is null) return items;
        var cacheItem = GetTypeCacheItem<T>();
        var identityTypeCacheItem = cacheItem.ColumnsToProperty.Where(x =>
            x.Key.Equals(identity.ColumnName, StringComparison.InvariantCultureIgnoreCase)).Select(x => x.Value).First();
        var identityType = identityTypeCacheItem.Type;
        var defaultIdentityValue = !identityType.IsGenericType ? Activator.CreateInstance(identityType) : null;
        var identityName = identityTypeCacheItem.Name;
        foreach (var item in items)
        {
            var value = cacheItem.TypeAccessor[item, identityName];
            if (!object.Equals(defaultIdentityValue, value)) 
                continue;
            if (reader.Read())
            {
                cacheItem.TypeAccessor[item, identityName] = Convert(reader[reader.GetOrdinal("Id")]);
            }
        }
        return items;
    }
    
    private static string GetFindPrimaryKeyInfoQuery(string tableName) =>
        $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{tableName}'";

    private static string GetFindIdentityInfoQuery(string tableName)
        => $"SELECT COLUMN_NAME as ColumnName, DATA_TYPE as Type FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{tableName}'";

    private static string CreateInsertOrUpdateMergeStatement(
        IEnumerable<string> columnNames,
        WriteToTempTableResult writeToTempTableResult)
    {
        var identityExist = writeToTempTableResult.Identity is not null;
        columnNames = identityExist ? columnNames.Where(x => x != writeToTempTableResult.Identity.ColumnName) : columnNames;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', columnNames.Select(x => $"[{x}]"));

        if (identityExist)
        {
            merge.Append($"DECLARE @Id TABLE ([Action] VARCHAR(20), [Id]");
            merge.Append(writeToTempTableResult.Identity.Type);
            merge.Append(')');
        }
        merge.Append($@"
MERGE [{writeToTempTableResult.TableName}] AS T  
USING (SELECT * FROM [{writeToTempTableResult.TempTable}]) AS S
ON ({string.Join(" AND ", writeToTempTableResult.PrimaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN NOT MATCHED
THEN INSERT ({columnsString}) VALUES ({columnsString})
WHEN MATCHED 
THEN UPDATE SET {string.Join(',', columnNames.Except(writeToTempTableResult.PrimaryKeys).Select(x => $"T.[{x}] = S.[{x}]"))}");
        if (identityExist)
        {
            merge.AppendLine();
            merge.Append($"OUTPUT $action, inserted.");
            merge.Append(writeToTempTableResult.Identity.ColumnName);
            merge.AppendLine(" INTO @Id ([Action], [Id]);");
            merge.AppendLine("SELECT [Id] FROM @Id WHERE [Action] = 'INSERT' ORDER BY [Id] ASC");
        }
        else
        {
            merge.AppendLine(";");
        }
        merge.AppendLine($"DROP TABLE {writeToTempTableResult.TempTable}");
        return merge.ToString();
    }

    private static bool OpenConnectionIfNot(IDbConnection connection)
    {
        if (connection.State == ConnectionState.Open) return false;
        connection.Open();

        return true;
    }

    private static string GetInsertMergeQuery(string columnsString, bool identityExist, WriteToTempTableResult createTmpTableResult)
    {
        var insert = new StringBuilder();
        if (identityExist)
        {
            insert.Append($"DECLARE @Id TABLE ([Id] {createTmpTableResult.Identity.Type})");
        }

        insert.Append($@"INSERT INTO [{createTmpTableResult.TableName}]({columnsString})");
        if (identityExist)
        {
            insert.AppendLine($"OUTPUT inserted.[{createTmpTableResult.Identity.ColumnName}] INTO @Id");
        }
        insert.AppendLine($"SELECT {columnsString} FROM {createTmpTableResult.TempTable}");
        if (identityExist)
        {
            insert.AppendLine("SELECT [Id] FROM @Id ORDER BY [Id] ASC");
        }
        insert.AppendLine($"DROP TABLE {createTmpTableResult.TempTable}");
        return insert.ToString();
    }

    private static string GetUpdateMergeQuery(IEnumerable<string> columnNames,  WriteToTempTableResult createTmpTableResult)
    {
        return $@"
MERGE [{createTmpTableResult.TableName}] AS T  
USING (SELECT * FROM [{createTmpTableResult.TempTable}]) AS S
ON ({string.Join(" AND ", createTmpTableResult.PrimaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN MATCHED 
THEN UPDATE SET {string.Join(',', columnNames.Except(createTmpTableResult.PrimaryKeys).Select(x => $"T.[{x}] = S.[{x}]"))};
DROP TABLE {createTmpTableResult.TempTable}";
    }
    
    private static string GetDeleteMergeQuery(WriteToTempTableResult createTmpTableResult) => $@"
MERGE [{createTmpTableResult.TableName}] AS T  
USING (SELECT * FROM [{createTmpTableResult.TempTable}]) AS S
ON ({string.Join(" AND ", createTmpTableResult.PrimaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN MATCHED
THEN DELETE;
DROP TABLE {createTmpTableResult.TempTable}";
    private static string GetAlterIdentityColumnQuery(string tempTableName,
       Identity identity) =>
        $@"ALTER TABLE {tempTableName} ADD _IfIdentityJustOneColumn_{identity.ColumnName} BIT
ALTER TABLE {tempTableName} DROP COLUMN {identity.ColumnName}
ALTER TABLE {tempTableName} ADD {identity.ColumnName} {identity.Type}
ALTER TABLE {tempTableName} DROP COLUMN _IfIdentityJustOneColumn_{identity.ColumnName}";
}