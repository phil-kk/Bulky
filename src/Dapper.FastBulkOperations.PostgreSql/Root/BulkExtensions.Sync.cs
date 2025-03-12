using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using FastMember;

namespace Bulky.Root;

public static partial class BulkExtensions
{
    private static bool OpenConnection(DbConnection connection)
    {
        if (connection.State is ConnectionState.Open) return false;
        connection.Open();
        return true;

    }
    
    public static  void BulkCopy<T>(IBulkWriter bulkWriter, DbConnection connection,
        DbTransaction transaction,
        IEnumerable<T> items,
        string tableName = default,
        int timeout = int.MaxValue,
        IEnumerable<string> excludeColumns = default,
        int batchSize = DefaultBatchSize)
    {
        var shouldCloseConnection =  OpenConnection(connection);
        var cacheItem = GetTypeCacheItem<T>();
        tableName ??= cacheItem.TableName;
        bulkWriter.Write(connection, transaction, timeout, batchSize,  items, excludeColumns is null ? cacheItem.ColumnsToProperty : cacheItem.ColumnsToProperty.ExceptBy(excludeColumns, x => x.Key), tableName);
        
        if (shouldCloseConnection) connection.Close();
    }

     public static  void BulkInsertOrUpdate<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
            IEnumerable<T> items,
            string tableName = default,
            DbTransaction transaction = default,
            int batchSize = DefaultBatchSize,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue)
    {
        var shouldCloseConnection = OpenConnection(connection);

        var cacheItem = GetTypeCacheItem<T>();
        tableName ??= cacheItem.TableName;
        var columnNames = cacheItem.ColumnNames.Where(x => excludeProperties?.Contains(x) is not true).Select(x => x).ToArray();
        var result = WriteToTemp(bulkWriter, dialect, connection, transaction, items, cacheItem.ColumnsToProperty, tableName,  batchSize,
            primaryKeys ?? cacheItem.PrimaryKeys, timeout);
        
        var merge = dialect.GetInsertOrUpdateMergeStatement(columnNames, result);
        
        if (result.Identity is null)
        {
            Execute(connection, merge, transaction);
            if (shouldCloseConnection) connection.Close();
            return;
        }
        using (var reader = ExecuteReader(connection, merge, transaction))
        {
            MapIdentity(items, reader, result.Identity);
        }
        if (shouldCloseConnection) connection.Close();
    }

     private static  BulkWriteContext WriteToTemp<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         DbTransaction transaction,
         IEnumerable<T> items,
         IEnumerable<KeyValuePair<string, Member>> columnMappings,
         string tableName,
         int batchSize = DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var tempTable = dialect.GetTempTableName(tableName);

         primaryKeys ??= FindPrimaryKeysInfo(dialect, connection, transaction, tableName);
         
         var identity = FindIdentityInfo(dialect, connection, transaction, tableName);
         CreateTemporaryTable(dialect, connection, transaction, identity, tableName, tempTable, columnMappings.Select(x => x.Key));

         bulkWriter.Write(connection, transaction, timeout, batchSize, items, columnMappings, tempTable);

         return new BulkWriteContext(tableName, tempTable,  primaryKeys, identity);
     }
     public static void BulkInsert<T>(IBulkWriter bulkWriter, ISqlDialect dialect, 
         DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var shouldCloseConnection = OpenConnection(connection);
         
         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Where<string>(x => !excludeProperties.Contains(x));
         var result = WriteToTemp(bulkWriter, dialect, connection,  transaction, items, cacheItem.ColumnsToProperty, tableName, batchSize,
             primaryKeys ?? cacheItem.PrimaryKeys, timeout);
         var identityExist = result.Identity is not null;

         var insert = dialect.GetInsertQuery(columnNames, result);
         if (!identityExist)
         {
             Execute(connection, insert, transaction);
             if (shouldCloseConnection) connection.Close();
             return;
         }
         using (var reader = ExecuteReader(connection, insert, transaction))
         {
             MapIdentity(items, reader, result.Identity);
         }
         if (shouldCloseConnection) connection.Close();
     }
     
     public static  void BulkUpdate<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var shouldCloseConnection = OpenConnection(connection);

         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Where(x => !excludeProperties.Contains(x)).Select(x => x);

         var result = WriteToTemp(bulkWriter, 
             dialect, 
             connection,  
             transaction, 
             items, 
             cacheItem.ColumnsToProperty, 
             tableName, 
             batchSize,
             primaryKeys ?? cacheItem.PrimaryKeys, 
             timeout);
         columnNames = result.Identity is null ? columnNames : columnNames.Where(x => x != result.Identity.ColumnName);
         Execute(connection, dialect.GetUpdateQuery(columnNames, result), transaction);
         if (shouldCloseConnection) connection.Close();
     }
     
     public static void BulkDelete<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var shouldCloseConnection = OpenConnection(connection);

         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         primaryKeys ??= cacheItem.PrimaryKeys ?? FindPrimaryKeysInfo(dialect, connection, transaction, tableName);
         var result = WriteToTemp(bulkWriter, 
             dialect, 
             connection, 
             transaction, 
             items, 
             cacheItem.ColumnsToProperty
                 .Where(x => primaryKeys.Contains(x.Key)), 
             tableName, 
             batchSize,
             primaryKeys, 
             timeout);
         var query = dialect.GetDeleteQuery(result);
         Execute(connection, query, transaction);
         if (shouldCloseConnection) connection.Close();
     }
     
    private static  void CreateTemporaryTable(ISqlDialect dialect, DbConnection connection, 
        DbTransaction transaction, 
        Identity identity, 
        string tableName, 
        string tempTableName, 
        IEnumerable<string> columnNames = default)
    {
        var queryString = new StringBuilder(dialect.GetCreateTempTableQuery(tempTableName, tableName, columnNames));
        if (identity is not null)
        {
            queryString.AppendLine(dialect.GetAlterIdentityColumnQuery(tempTableName, identity));
        }

        Execute(connection, queryString.ToString(), transaction);
    }

    private static Identity FindIdentityInfo(ISqlDialect dialect, DbConnection connection, DbTransaction transaction,
        string tableName)
    {
        using var reader = ExecuteReader(connection, dialect.GetFindIdentityQuery(connection.Database, tableName), transaction);
        return reader.Read() ? new Identity(reader[0].ToString(), reader[1].ToString()) : null;
    }

    private static IEnumerable<string> FindPrimaryKeysInfo(ISqlDialect dialect, DbConnection connection,
        DbTransaction transaction, string tableName)
    {
        using var reader = ExecuteReader(connection, dialect.GetFindPrimaryKeysQuery(connection.Database, tableName), transaction);
        while (reader.Read())
        {
            yield return reader[0].ToString();
        }
    }

    private static void Execute(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        command.ExecuteNonQuery();
    }
    
    private static DbDataReader ExecuteReader(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command.ExecuteReader();
    }
}