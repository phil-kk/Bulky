using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using Dapper;
using FastMember;

namespace Dapper.FastBulkOperations.SqlServer;

public static partial class BulkExtensions
{
    private record WriteToTempTableResult(
        string TableName,
        string TempTable,
        IEnumerable<string> PrimaryKeys,
        Identity Identity);
    public static  void BulkCopy<T>(this IDbConnection connection,
        IList<T> items,
        string tableName = default,
        IDbTransaction transaction = default,
        IEnumerable<string> excludeColumns = default,
        int? batchSize = default)
    {
        if (items.Count is 0) return;
        var shouldCloseConnection =  OpenConnectionIfNot(connection);
        var cacheItem = GetTypeCacheItem<T>();
        tableName ??= cacheItem.TableName;
        using var bulkCopy = new BulkWriter<T>(items, connection, transaction, batchSize ?? items.Count, int.MaxValue, tableName, excludeColumns is null ? cacheItem.ColumnsToProperty : cacheItem.ColumnsToProperty.ExceptBy(excludeColumns, x => x.Key), cacheItem.PropertyNames);
        bulkCopy.Write();
        
        if (shouldCloseConnection) connection.Close();
    }

     public static  void BulkInsertOrUpdate<T>(this IDbConnection connection,
            IList<T> items,
            string tableName = default,
            IDbTransaction transaction = default,
            int? batchSize = default,
            int bulkCopyTimeout = int.MaxValue,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue)
    {
        var shouldCloseConnection = OpenConnectionIfNot(connection);

        var cacheItem = GetTypeCacheItem<T>();
        tableName ??= cacheItem.TableName;
        var columnNames = cacheItem.ColumnNames.Where(x => excludeProperties?.Contains(x) is not true).Select(x => x).ToArray();
        var result = WriteToTemp(connection, items, cacheItem.ColumnsToProperty, cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
            primaryKeys ?? cacheItem.PrimaryKeys, timeout);
        
        var merge = CreateInsertOrUpdateMergeStatement(columnNames, result);
        using (var reader = connection.ExecuteReader(merge, transaction:transaction, commandTimeout:timeout))
        {
            MapIdentity(items, reader, result.Identity);
        }
        if (shouldCloseConnection) connection.Close();
    }

     private static  WriteToTempTableResult WriteToTemp<T>(IDbConnection connection,
         ICollection<T> items,
         Dictionary<string, Member> columnMappings,
         string[] propertyNames,
         string tableName,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var tempTable = $"{tableName}_{Guid.NewGuid():N}";

         primaryKeys ??= FindPrimaryKeysInfo(connection, transaction, tableName);
         
         var identity = FindIdentityInfo(connection, transaction, tableName);
         CreateTemporaryTable(connection, transaction, identity, tableName, tempTable, timeout, columnMappings.Keys);

         using (var bulkWriter = new BulkWriter<T>(items, connection, transaction, batchSize ?? items.Count, bulkCopyTimeout, tempTable, columnMappings, propertyNames))
         {
             bulkWriter.Write();
         }

         return new WriteToTempTableResult(tableName, tempTable,  primaryKeys, identity);
     }
     public static void BulkInsert<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         if (items.Count is 0) return;

         var shouldCloseConnection = OpenConnectionIfNot(connection);
         
         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Where(x => !excludeProperties.Contains(x));
         var result = WriteToTemp(connection, items, cacheItem.ColumnsToProperty, cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
             primaryKeys ?? cacheItem.PrimaryKeys, timeout);
         var identityExist = result.Identity is not null;
         var columnsString = string.Join(',', !identityExist ? columnNames : columnNames.Where(x => result.Identity.ColumnName != x).Select(x => $"[{x}]"));

         using (var reader = connection.ExecuteReader(GetInsertMergeQuery(columnsString, identityExist, result), transaction:transaction, commandTimeout:timeout))
         {
             MapIdentity(items, reader, result.Identity);
         }
         if (shouldCloseConnection) connection.Close();
     }
     
     public static  void BulkUpdate<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         if (items.Count is 0) return;

         var shouldCloseConnection = OpenConnectionIfNot(connection);

         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Where(x => !excludeProperties.Contains(x)).Select(x => x);

         var result = WriteToTemp(connection, items, cacheItem.ColumnsToProperty, cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
             primaryKeys ?? cacheItem.PrimaryKeys, timeout);
         columnNames = result.Identity is null ? columnNames : columnNames.Where(x => x != result.Identity.ColumnName);
         connection.Execute(GetUpdateMergeQuery(columnNames, result), transaction: transaction, commandTimeout: timeout);
         if (shouldCloseConnection) connection.Close();
     }
     
     public static void BulkDelete<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         if (items.Count is 0) return;

         var shouldCloseConnection = OpenConnectionIfNot(connection);

         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         primaryKeys ??= cacheItem.PrimaryKeys ?? FindPrimaryKeysInfo(connection, transaction, tableName);
         var result = WriteToTemp(connection, items, cacheItem.ColumnsToProperty.Where(x => primaryKeys.Contains(x.Key)).ToDictionary(x => x.Key, x=> x.Value), cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
             primaryKeys, timeout);
         
         connection.Execute(GetDeleteMergeQuery(result), transaction: transaction, commandTimeout: timeout);
         if (shouldCloseConnection) connection.Close();
     }
     
    private static  void CreateTemporaryTable(IDbConnection connection, 
        IDbTransaction transaction, 
        Identity identityInfo, 
        string destination, 
        string tempTableName, 
        int timeout,
        IEnumerable<string> columnNames = default)
    {
        var queryString = new StringBuilder(GetCreateTempTableQuery(tempTableName, destination, columnNames));
        if (identityInfo is null)
        {
            connection.Execute(queryString.ToString(), transaction: transaction, commandTimeout:timeout);
            return;
        }
        queryString.AppendLine(GetAlterIdentityColumnQuery(tempTableName, identityInfo));
        connection.Execute(queryString.ToString(), transaction: transaction);
    }

    private static Identity FindIdentityInfo(IDbConnection connection, IDbTransaction transaction, string tableName) => 
        connection.QuerySingleOrDefault<Identity>(GetFindIdentityInfoQuery(tableName), transaction: transaction);
    
    private static  IEnumerable<string> FindPrimaryKeysInfo(IDbConnection connection, IDbTransaction transaction, string tableName) =>
        connection.Query<string>(GetFindPrimaryKeyInfoQuery(tableName), transaction: transaction);
}