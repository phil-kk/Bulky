using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastMember;

namespace BulkyMerge.Root;

public static partial class BulkExtensions
{
    public static async Task BulkCopyAsync<T>(IBulkWriter bulkWriter, DbConnection connection,
        DbTransaction transaction,
        IEnumerable<T> items,
        string tableName = default,
        IEnumerable<string> excludeColumns = default,
        int timeout = int.MaxValue,
        int batchSize = DefaultBatchSize)
    {
        var shouldCloseConnection =  await OpenConnectionAsync(connection);
        
        var cacheItem = GetTypeCacheItem<T>();
        tableName ??= cacheItem.TableName;

        await bulkWriter.WriteAsync(connection, 
            transaction,  
            timeout,
            batchSize, 
            items, 
            cacheItem.ColumnsToProperty, 
            tableName);
        if (shouldCloseConnection) await connection.CloseAsync();
    }

     public static async Task BulkInsertOrUpdateAsync<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
            IList<T> items,
            string tableName = default,
            DbTransaction transaction = default,
            int batchSize = DefaultBatchSize,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue)
    {
        var shouldCloseConnection = await OpenConnectionAsync(connection);

        var cacheItem = GetTypeCacheItem<T>();
        tableName ??= cacheItem.TableName;
        var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Except(excludeProperties).ToArray();

        var result = await WriteToTempAsync(bulkWriter, 
            dialect, 
            connection, 
            transaction, 
            items, 
            cacheItem.ColumnsToProperty, 
            tableName,  
            batchSize, 
            primaryKeys ?? cacheItem.PrimaryKeys, 
            timeout);
        
        var merge = dialect.GetInsertOrUpdateMergeStatement(columnNames, result);

        await using (var reader = await ExecuteReaderAsync(connection, merge, transaction))
        {
            MapIdentity(items, reader, result.Identity);
        }
        if (shouldCloseConnection) await connection.CloseAsync();
    }
     
     private static async Task<BulkWriteContext> WriteToTempAsync<T>(IBulkWriter bulkWriter, 
         ISqlDialect dialect,
         DbConnection connection,
         DbTransaction transaction,
         IEnumerable<T> items,
         IEnumerable<KeyValuePair<string, Member>> columnMappings,
         string tableName,
         int batchSize = DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {

         var tempTable = dialect.GetTempTableName(tableName);

         primaryKeys ??= await FindPrimaryKeysInfoAsync(dialect, connection, transaction, tableName);
         
         var identity = await FindIdentityInfoAsync(dialect, connection, transaction, tableName);
         
         await CreateTemporaryTableAsync(dialect, connection, transaction, identity, tableName, tempTable, columnMappings.Select(x => x.Key));
         await bulkWriter.WriteAsync(connection, transaction, timeout, batchSize, items, columnMappings, tempTable);

         return new BulkWriteContext(tableName, tempTable,  primaryKeys, identity);
     }
     public static async Task BulkInsertAsync<T>(IBulkWriter bulkWriter, ISqlDialect dialect, 
         DbConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var shouldCloseConnection = await OpenConnectionAsync(connection);
         
         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Where<string>(x => !excludeProperties.Contains(x));
         var result = await WriteToTempAsync(bulkWriter, 
             dialect, 
             connection, 
             transaction, 
             items, 
             cacheItem.ColumnsToProperty, 
             tableName,  
             batchSize, 
             primaryKeys ?? cacheItem.PrimaryKeys, 
             timeout);
         var merge = dialect.GetInsertQuery(columnNames, result);

         if (result.Identity is null)
         {
             await ExecuteAsync(connection, merge, transaction);
             if (shouldCloseConnection) await connection.CloseAsync();
             return;
         }

         await using (var reader = await ExecuteReaderAsync(connection, merge, transaction))
         {
             MapIdentity(items, reader, result.Identity);
         }
         if (shouldCloseConnection) await connection.CloseAsync();
     }
     
     public static async Task BulkUpdateAsync<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var shouldCloseConnection = await OpenConnectionAsync(connection);

         var cacheItem = GetTypeCacheItem<T>();
         tableName ??= cacheItem.TableName;
         var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Where<string>(x => !excludeProperties.Contains(x)).Select(x => x);

         var result = await WriteToTempAsync(bulkWriter, 
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
         var sql = dialect.GetUpdateQuery(columnNames, result);
         await ExecuteAsync(connection, sql, transaction);
         if (shouldCloseConnection) await connection.CloseAsync();
     }
     
     public static async Task BulkDeleteAsync<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     {
         var shouldCloseConnection = await OpenConnectionAsync(connection);
         var cacheItem = GetTypeCacheItem<T>();
         
         tableName ??= cacheItem.TableName;
         primaryKeys ??= cacheItem.PrimaryKeys ?? await FindPrimaryKeysInfoAsync(dialect, connection, transaction, tableName);
         var result = await WriteToTempAsync(bulkWriter, 
             dialect, 
             connection, 
             transaction, 
             items,  
             cacheItem.ColumnsToProperty.Where(x => primaryKeys.Contains(x.Key)), 
             tableName, 
             batchSize,
             primaryKeys, timeout);
         
         await ExecuteAsync(connection,dialect.GetDeleteQuery(result), transaction: transaction);
         if (shouldCloseConnection) await connection.CloseAsync();
     }
     
    private static Task CreateTemporaryTableAsync(ISqlDialect dialect, DbConnection connection, 
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

        return ExecuteAsync(connection, queryString.ToString(), transaction);
    }

    private static async Task<Identity> FindIdentityInfoAsync(ISqlDialect dialect, DbConnection connection, DbTransaction transaction, string tableName)
    {
        await using var reader = await ExecuteReaderAsync(connection, dialect.GetFindIdentityQuery(connection.Database, tableName), transaction);
        return await reader.ReadAsync() ? new Identity(reader[0].ToString(), reader[1].ToString()) : null;
    }
    
    private static async Task<IEnumerable<string>> FindPrimaryKeysInfoAsync(ISqlDialect dialect, DbConnection connection, DbTransaction transaction, string tableName)
    {
        var result = new List<string>();
        await using var reader = await ExecuteReaderAsync(connection,dialect.GetFindPrimaryKeysQuery(connection.Database, tableName), transaction);
        while (await reader.ReadAsync())
        {
            result.Add(reader[0].ToString());
        }

        return result;
    }
    
    private static async Task<bool> OpenConnectionAsync(DbConnection connection)
    {
        if (connection.State is ConnectionState.Open) return false;
        await connection.OpenAsync();
        return true;
    }
    
    private static async Task ExecuteAsync(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        await command.ExecuteNonQueryAsync();
    }
    
    private static async Task<DbDataReader> ExecuteReaderAsync(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return await command.ExecuteReaderAsync();
    }
}