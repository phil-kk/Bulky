using System.Collections.Generic;
using Bulky.Root;
using MySqlConnector;

namespace Bulky.MySql;

public static partial class MySqlBulkExtensions
{
    public static  void BulkCopy<T>(this MySqlConnection connection,
        IEnumerable<T> items,
        string tableName = default,
        MySqlTransaction transaction = default,
        IEnumerable<string> excludeColumns = default,
        int timeout = int.MaxValue,
        int batchSize = BulkExtensions.DefaultBatchSize)
    => BulkExtensions.BulkCopy(BulkWriter, connection, transaction, items, tableName, timeout, excludeColumns, batchSize);

     public static  void BulkInsertOrUpdate<T>(this MySqlConnection connection,
            IEnumerable<T> items,
            string tableName = default,
            MySqlTransaction transaction = default,
            int batchSize = BulkExtensions.DefaultBatchSize,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue)
     => BulkExtensions.BulkInsertOrUpdate(BulkWriter, 
         Dialect, 
         connection, 
         items, 
         tableName, 
         transaction, 
         batchSize,
         excludeProperties, 
         primaryKeys, 
         timeout);

     public static void BulkInsert<T>(this MySqlConnection connection,
         IList<T> items,
         string tableName = default,
         MySqlTransaction transaction = default,
         int batchSize = BulkExtensions.DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => BulkExtensions.BulkInsert(BulkWriter, 
         Dialect, 
         connection, 
         items, 
         tableName, 
         transaction, 
         batchSize,
         excludeProperties, 
         primaryKeys, 
         timeout);
     
     public static  void BulkUpdate<T>(this MySqlConnection connection,
         IList<T> items,
         string tableName = default,
         MySqlTransaction transaction = default,
         int batchSize = BulkExtensions.DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => BulkExtensions.BulkUpdate(BulkWriter, 
         Dialect, 
         connection, 
         items, 
         tableName, 
         transaction, 
         batchSize,
         excludeProperties, 
         primaryKeys, 
         timeout);
     
     public static void BulkDelete<T>(this MySqlConnection connection,
         IList<T> items,
         string tableName = default,
         MySqlTransaction transaction = default,
         int batchSize = BulkExtensions.DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => BulkExtensions.BulkDelete(BulkWriter, 
         Dialect, 
         connection, 
         items, 
         tableName, 
         transaction, 
         batchSize,
         primaryKeys, 
         timeout);
}