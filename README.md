Dapper.Contrib - fast BulkInsert, BulkUpdate, BulkInsertOrUpdate, BulkCopy and BulkDelete extensions
========================================

[![Build status](https://ci.appveyor.com/api/projects/status/iylj7wjrak5866i6?svg=true)](https://ci.appveyor.com/project/filipppka/dapper-fastbulkoperations)


Depends on [Dapper](https://www.nuget.org/packages/Dapper) [FastMember](https://www.nuget.org/packages/FastMember)

Simple usage :

```csharp
using Dapper;
using Dapper.FastBulkOperations.SqlServer;
using Microsoft.Data.SqlClient; // or System.Data.SqlClient

const string connectionString = "Server=localhost;Database=tempdb;Trusted_Connection=True;TrustServerCertificate=true;";

await using var create = new SqlConnection(connectionString);
{
    create.Execute($"IF OBJECT_ID('Person', 'U') IS NOT NULL DROP TABLE [Person]");
    create.Execute("CREATE TABLE [Person] ([IdentityId] INT NOT NULL IDENTITY(1,1) PRIMARY KEY, [FullName] NVARCHAR(255) NOT NULL)");
}

var people = new List<Person> { new Person { FullName = "A B"}, new Person { FullName = "C D"}};

await using var sqlConnection = new SqlConnection(connectionString);
sqlConnection.BulkInsert(people);

foreach (var person in people)
{
    Console.WriteLine($"IdentityId : {person.IdentityId} FullName : {person.FullName}");
}
// No need in any Mapping code, Primary Keys and Identity will be found automatically
public class Person
{
    public int IdentityId { get; set; }
    
    public string FullName { get; set; }
}
```
Please check samples folder

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


|         Method |        Job |                Toolchain | IterationCount | LaunchCount | RunStrategy | UnrollFactor | WarmupCount |       Mean |       Error |   StdDev |   StdErr |        Min |         Q1 |     Median |         Q3 |        Max |   Op/s |       Gen0
 |      Gen1 | Allocated |
|--------------- |----------- |------------------------- |--------------- |------------ |------------ |------------- |------------ |-----------:|------------:|---------:|---------:|-----------:|-----------:|-----------:|-----------:|-----------:|-------:|-----------
:|----------:|----------:|
| BulkExtensions | Job-CELPXL |                  Default |        Default |     Default |  Monitoring |            1 |     Default |   740.9 ms |    65.98 ms | 43.64 ms | 13.80 ms |   684.2 ms |   722.1 ms |   731.9 ms |   772.9 ms |   805.6 ms | 1.3497 |  3000.0000
 |         - |  24.44 MB |
|     DapperPlus | Job-CELPXL |                  Default |        Default |     Default |  Monitoring |            1 |     Default | 1,359.8 ms |    81.15 ms | 53.68 ms | 16.97 ms | 1,297.0 ms | 1,304.0 ms | 1,371.0 ms | 1,404.3 ms | 1,437.6 ms | 0.7354 | 14000.0000
 | 2000.0000 | 119.46 MB |
| BulkExtensions |   ShortRun | InProcessNoEmitToolchain |              3 |           1 |     Default |           16 |           3 |   717.9 ms |   643.58 ms | 35.28 ms | 20.37 ms |   694.3 ms |   697.7 ms |   701.0 ms |   729.8 ms |   758.5 ms | 1.3929 |  3000.0000
 |         - |  24.44 MB |
|     DapperPlus |   ShortRun | InProcessNoEmitToolchain |              3 |           1 |     Default |           16 |           3 | 1,334.8 ms | 1,053.08 ms | 57.72 ms | 33.33 ms | 1,297.8 ms | 1,301.6 ms | 1,305.4 ms | 1,353.4 ms | 1,401.4 ms | 0.7491 | 14000.0000
 | 2000.0000 | 119.46 MB |
