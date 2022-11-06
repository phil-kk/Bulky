using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using FastMember;

namespace Dapper.FastBulkOperations.SqlServer;

public static partial class BulkExtensions
{
    public static async Task BulkCopyAsync<T>(this IDbConnection connection,
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

        await using var bulkCopy = new BulkWriter<T>(items, connection, transaction, batchSize ?? items.Count, int.MaxValue, tableName, excludeColumns is null ? cacheItem.ColumnsToProperty : cacheItem.ColumnsToProperty.ExceptBy(excludeColumns, x => x.Key), cacheItem.PropertyNames);
        await bulkCopy.WriteAsync();
        
        if (shouldCloseConnection) connection.Close();
    }

     public static async Task BulkInsertOrUpdateAsync<T>(this IDbConnection connection,
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
        var columnNames = excludeProperties is null ? cacheItem.ColumnNames : cacheItem.ColumnNames.Except(excludeProperties).ToArray();

        var result = await WriteToTempAsync(connection, items, cacheItem.ColumnsToProperty, cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
            primaryKeys ?? cacheItem.PrimaryKeys, timeout);
        
        var merge = CreateInsertOrUpdateMergeStatement(columnNames, result);
        using (var reader = await connection.ExecuteReaderAsync(merge, transaction:transaction, commandTimeout:timeout))
        {
            MapIdentity(items, reader, result.Identity);
        }
        if (shouldCloseConnection) connection.Close();
    }
     
     private static async Task<WriteToTempTableResult> 
     WriteToTempAsync<T>(IDbConnection connection,
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

         var tempTable = $"#{tableName}_{Guid.NewGuid():N}";

         primaryKeys ??= await FindPrimaryKeysInfoAsync(connection, transaction, tableName);
         
         var identity = await FindIdentityInfoAsync(connection, transaction, tableName);
         
         await CreateTemporaryTableAsync(connection, transaction, identity, tableName, tempTable, timeout, columnMappings.Keys);
         await using (var bulkWriter = new BulkWriter<T>(items, connection, transaction, batchSize ?? items.Count, bulkCopyTimeout, tempTable, columnMappings, propertyNames))
         {
             await bulkWriter.WriteAsync();
         }

         return new WriteToTempTableResult(tableName, tempTable,  primaryKeys, identity);
     }
     public static async Task BulkInsertAsync<T>(this IDbConnection connection,
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
         var result = await WriteToTempAsync(connection, items, cacheItem.ColumnsToProperty, cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
             primaryKeys ?? cacheItem.PrimaryKeys, timeout);
         var identityExist = result.Identity is not null;
         var columnsString = string.Join(',', !identityExist ? columnNames : columnNames.Where(x => result.Identity.ColumnName != x).Select(x => $"[{x}]"));

         using (var reader = await connection.ExecuteReaderAsync(GetInsertMergeQuery(columnsString, identityExist, result), transaction:transaction, commandTimeout:timeout))
         {
             MapIdentity(items, reader, result.Identity);
         }
         if (shouldCloseConnection) connection.Close();
     }
     
     public static async Task BulkUpdateAsync<T>(this IDbConnection connection,
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

         var result = await WriteToTempAsync(connection, items, cacheItem.ColumnsToProperty, cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
             primaryKeys ?? cacheItem.PrimaryKeys, timeout);
         columnNames = result.Identity is null ? columnNames : columnNames.Where(x => x != result.Identity.ColumnName);
         await connection.ExecuteAsync(GetUpdateMergeQuery(columnNames, result), transaction: transaction, commandTimeout: timeout);
         if (shouldCloseConnection) connection.Close();
     }
     
     public static async Task BulkDeleteAsync<T>(this IDbConnection connection,
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
         primaryKeys ??= cacheItem.PrimaryKeys ?? await FindPrimaryKeysInfoAsync(connection, transaction, tableName);
         var result = await WriteToTempAsync(connection, items, cacheItem.ColumnsToProperty.IntersectBy(primaryKeys, x => x.Key).ToDictionary(x => x.Key, x=> x.Value), cacheItem.PropertyNames, tableName, transaction, batchSize, bulkCopyTimeout,
             primaryKeys, timeout);
         
         await connection.ExecuteAsync(GetDeleteMergeQuery(result), transaction: transaction, commandTimeout: timeout);
         if (shouldCloseConnection) connection.Close();
     }
     
    private static async Task CreateTemporaryTableAsync(IDbConnection connection, 
        IDbTransaction transaction, 
        Identity identity, 
        string tableName, 
        string tempTableName, 
        int timeout,
        IEnumerable<string> columnNames = default)
    {
        var queryString = new StringBuilder(GetCreateTempTableQuery(tempTableName, tableName, columnNames));
        if (identity is null)
        {
            await connection.ExecuteAsync(queryString.ToString(), transaction: transaction, commandTimeout:timeout);
            return;
        }
        queryString.AppendLine();
        queryString.AppendLine(GetAlterIdentityColumnQuery(tempTableName, identity));
        await connection.ExecuteAsync(queryString.ToString(), transaction: transaction);
    }

    private static async Task<Identity> FindIdentityInfoAsync(IDbConnection connection, IDbTransaction transaction, string tableName) => 
        await connection.QuerySingleOrDefaultAsync<Identity>(GetFindIdentityInfoQuery(tableName), transaction: transaction);
    
    private static async Task<IEnumerable<string>> FindPrimaryKeysInfoAsync(IDbConnection connection, IDbTransaction transaction, string tableName) =>
        await connection.QueryAsync<string>(GetFindPrimaryKeyInfoQuery(tableName), transaction: transaction);
}