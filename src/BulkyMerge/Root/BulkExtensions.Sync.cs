using System.Collections.Generic;
using System.Data.Common;

namespace BulkyMerge.Root;

internal static partial class BulkExtensions
{
    internal static void BulkCopy<T>(IBulkWriter bulkWriter, DbConnection connection,
        DbTransaction transaction,
        IEnumerable<T> items,
        string tableName = default,
        IEnumerable<string> excludeColumns = default,
        int timeout = int.MaxValue,
        int batchSize = DefaultBatchSize)
    => BulkCopyAsync(bulkWriter, connection, transaction, items, tableName, excludeColumns, timeout, batchSize).GetAwaiter().GetResult();

    internal static  void BulkInsertOrUpdate<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
            IEnumerable<T> items,
            string tableName = default,
            DbTransaction transaction = default,
            int batchSize = DefaultBatchSize,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue)
    => BulkInsertOrUpdateAsync(bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout).GetAwaiter().GetResult();

    internal static void BulkInsert<T>(IBulkWriter bulkWriter, ISqlDialect dialect, 
         DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
    => BulkInsertAsync(bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout).GetAwaiter().GetResult();

    internal static  void BulkUpdate<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => BulkUpdateAsync(bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout).GetAwaiter().GetResult();

    internal static void BulkDelete<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => BulkDeleteAsync(bulkWriter, dialect, connection, items, tableName, transaction, batchSize, primaryKeys, timeout).GetAwaiter().GetResult();
}