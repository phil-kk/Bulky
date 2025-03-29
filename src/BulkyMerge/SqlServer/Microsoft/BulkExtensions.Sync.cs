using System.Collections.Generic;
using BulkyMerge.Root;
using Microsoft.Data.SqlClient;

namespace BulkyMerge.SqlServer;

public static partial class SqlServerBulkExtensions
{
    public static  void BulkCopy<T>(this SqlConnection connection,
        IEnumerable<T> items,
        string tableName = default,
        SqlTransaction transaction = default,
        IEnumerable<string> excludeColumns = default,
        int timeout = int.MaxValue,
        int batchSize = BulkExtensions.DefaultBatchSize)
    => BulkExtensions.BulkCopy(BulkWriter, connection, transaction, items, tableName, excludeColumns, timeout, batchSize);

     public static  void BulkInsertOrUpdate<T>(this SqlConnection connection,
            IList<T> items,
            string tableName = default,
            SqlTransaction transaction = default,
            int batchSize = BulkExtensions.DefaultBatchSize,
            int bulkCopyTimeout = int.MaxValue,
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

     public static void BulkInsert<T>(this SqlConnection connection,
         IList<T> items,
         string tableName = default,
         SqlTransaction transaction = default,
         int batchSize = BulkExtensions.DefaultBatchSize,
         int bulkCopyTimeout = int.MaxValue,
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
     
     public static  void BulkUpdate<T>(this SqlConnection connection,
         IList<T> items,
         string tableName = default,
         SqlTransaction transaction = default,
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
     
     public static void BulkDelete<T>(this SqlConnection connection,
         IList<T> items,
         string tableName = default,
         SqlTransaction transaction = default,
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