Dapper.Contrib - fast BulkInsert, BulkUpdate, BulkInsertOrUpdate, BulkCopy and BulkDelete extensions
========================================

[![Build status](https://ci.appveyor.com/api/projects/status/iylj7wjrak5866i6?svg=true)](https://ci.appveyor.com/project/filipppka/dapper-fastbulkoperations)


Depends on [Dapper](https://www.nuget.org/packages/Dapper) [FastMember](https://www.nuget.org/packages/FastMember)

Simple usage 

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


