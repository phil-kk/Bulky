DappertBulkOperations - fast BulkInsert, BulkUpdate, BulkInsertOrUpdate, BulkCopy and BulkDelete extensions
========================================

[![Build status](https://ci.appveyor.com/api/projects/status/iylj7wjrak5866i6?svg=true)](https://ci.appveyor.com/project/filipppka/dapper-fastbulkoperations)

Depends on [Dapper](https://www.nuget.org/packages/Dapper) [FastMember](https://www.nuget.org/packages/FastMember)

Simple usage :

```csharp
const string connectionString = "Server=localhost,1433;Database=master;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=True;";

TypeConverters.RegisterTypeConverter(typeof(JsonObj), Newtonsoft.Json.JsonConvert.SerializeObject);

var people = new List<Person> { 
    new Person { FullName = "A B", JsonObj = new JsonObj { JsonProp = "test" } }, 
    new Person { FullName = "C D", JsonObj = new JsonObj { JsonProp = "test2" } },
    new Person { IdentityId = 3, FullName = "F E", JsonObj = new JsonObj { JsonProp = "test3" } }
};

await using var sqlConnection = new SqlConnection(connectionString); // MysqlConenction or NpgsqlConnection
sqlConnection.BulkInsertOrUpdate(people, tableName:"Person");


foreach (var person in people)
{
    Console.WriteLine($"IdentityId : {person.IdentityId} FullName : {person.FullName}");
}
public class Person
{
    public int IdentityId { get; set; }

    public string FullName { get; set; }

    public JsonObj JsonObj { get; set; }
}
public class JsonObj
{
    public string JsonProp { get; set; }
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




