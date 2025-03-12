using System.Collections.Generic;
using System.Threading.Tasks;
using Bulky.Root;
using MySqlConnector;

namespace Bulky.MySql;

public static partial class MySqlBulkExtensions
{
    public static Task BulkCopyAsync<T>(this MySqlConnection connection,
        IEnumerable<T> items,
        string tableName = default,
        MySqlTransaction transaction = default,
        IEnumerable<string> excludeColumns = default,
        int timeout = int.MaxValue,
        int batchSize = BulkExtensions.DefaultBatchSize)
    => BulkExtensions.BulkCopyAsync(BulkWriter, connection, transaction, items, tableName, excludeColumns, timeout, batchSize);

    public static Task BulkInsertOrUpdateAsync<T>(this MySqlConnection connection,
        IList<T> items,
        string tableName = default,
        MySqlTransaction transaction = default,
        int batchSize = BulkExtensions.DefaultBatchSize,
        IEnumerable<string> excludeProperties = default,
        IEnumerable<string> primaryKeys = default,
        int timeout = int.MaxValue)
        => BulkExtensions.BulkInsertOrUpdateAsync(BulkWriter, 
            Dialect, 
            connection, 
            items, 
            tableName, 
            transaction,
            batchSize, 
            excludeProperties, 
            primaryKeys, 
            timeout);
     
     public static Task BulkInsertAsync<T>(this MySqlConnection connection,
         IList<T> items,
         string tableName = default,
         MySqlTransaction transaction = default,
         int batchSize =  BulkExtensions.DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => BulkExtensions.BulkInsertAsync(BulkWriter, 
         Dialect, 
         connection, 
         items, 
         tableName, 
         transaction,
         batchSize, 
         excludeProperties, 
         primaryKeys, 
         timeout);
     
     public static Task BulkUpdateAsync<T>(this MySqlConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         MySqlTransaction transaction = default,
         int batchSize = BulkExtensions.DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => BulkExtensions.BulkUpdateAsync(BulkWriter, 
         Dialect, 
         connection, 
         items, 
         tableName, 
         transaction,
         batchSize, 
         excludeProperties, 
         primaryKeys, 
         timeout);
     
     public static Task BulkDeleteAsync<T>(this MySqlConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         MySqlTransaction transaction = default,
         int batchSize = BulkExtensions.DefaultBatchSize,
         int bulkCopyTimeout = int.MaxValue,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
         => BulkExtensions.BulkDeleteAsync(BulkWriter, 
             Dialect, 
             connection, 
             items, 
             tableName, 
             transaction,
             batchSize, 
             primaryKeys, 
             timeout);
}