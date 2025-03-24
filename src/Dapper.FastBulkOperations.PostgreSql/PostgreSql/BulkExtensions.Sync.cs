using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using BulkyMerge.Root;
using Dapper;
using FastMember;
using Npgsql;

namespace BulkyMerge.PostgreSql;

public static partial class NpgsqlBulkExtensions
{
    public static  void BulkCopy<T>(this NpgsqlConnection connection,
        IEnumerable<T> items,
        string tableName = default,
        NpgsqlTransaction transaction = default,
        IEnumerable<string> excludeColumns = default,
        int timeout = int.MaxValue,
        int batchSize = BulkExtensions.DefaultBatchSize)
    => BulkExtensions.BulkCopy(BulkWriter, connection, transaction, items, tableName, timeout, excludeColumns, batchSize);

     public static  void BulkInsertOrUpdate<T>(this NpgsqlConnection connection,
            IEnumerable<T> items,
            string tableName = default,
            NpgsqlTransaction transaction = default,
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

     public static void BulkInsert<T>(this NpgsqlConnection connection,
         IList<T> items,
         string tableName = default,
         NpgsqlTransaction transaction = default,
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
     
     public static  void BulkUpdate<T>(this NpgsqlConnection connection,
         IList<T> items,
         string tableName = default,
         NpgsqlTransaction transaction = default,
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
     
     public static void BulkDelete<T>(this NpgsqlConnection connection,
         IList<T> items,
         string tableName = default,
         NpgsqlTransaction transaction = default,
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