Dapper.Contrib - fast BulkInsert, BulkUpdate, BulkInsertOrUpdate, BulkCopy and BulkDelete extensions
========================================

Build status

[![Build status](https://ci.appveyor.com/api/projects/status/iylj7wjrak5866i6?svg=true)](https://ci.appveyor.com/project/filipppka/dapper-fastbulkoperations)


Depends on [Dapper](https://www.nuget.org/packages/Dapper) [FastMember](https://www.nuget.org/packages/FastMember)

API :

```csharp
void BulkCopy<T>(this IDbConnection connection,
        IList<T> items,
        string tableName = default,
        IDbTransaction transaction = default,
        IEnumerable<string> excludeColumns = default,
        int? batchSize = default);

Task BulkCopyAsync<T>(this IDbConnection connection,
        IList<T> items,
        string tableName = default,
        IDbTransaction transaction = default,
        IEnumerable<string> excludeColumns = default,
        int? batchSize = default);
        
 void BulkInsert<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue);
         
 Task BulkInsertAsync<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue);
         
void BulkUpdate<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue);
         
Task BulkUpdateAsync<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue);         
        
void BulkInsertOrUpdate<T>(this IDbConnection connection,
            IList<T> items,
            string tableName = default,
            IDbTransaction transaction = default,
            int? batchSize = default,
            int bulkCopyTimeout = int.MaxValue,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue);
            
 Task BulkInsertOrUpdateAsync<T>(this IDbConnection connection,
            IList<T> items,
            string tableName = default,
            IDbTransaction transaction = default,
            int? batchSize = default,
            int bulkCopyTimeout = int.MaxValue,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue);
            
 void BulkDelete<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue);
         
 Task BulkDeleteAsync<T>(this IDbConnection connection,
         IList<T> items,
         string tableName = default,
         IDbTransaction transaction = default,
         int? batchSize = default,
         int bulkCopyTimeout = int.MaxValue,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue);        
```


