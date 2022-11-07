using System.Data;
using System.Text.Json;
using Dapper;
using Dapper.FastBulkOperations.SqlServer;
using Microsoft.Data.SqlClient; // or System.Data.SqlClient
SqlMapper.AddTypeHandler(new JsonTypeHandler<FullName>());
const string connectionString = "Server=localhost;Database=tempdb;Trusted_Connection=True;TrustServerCertificate=true;";

await using var create = new SqlConnection(connectionString);
{
    create.Execute($"IF OBJECT_ID('Person', 'U') IS NOT NULL DROP TABLE [Person]");
    create.Execute("CREATE TABLE [Person] ([IdentityId] INT NOT NULL IDENTITY(1,1) PRIMARY KEY, [FullName] NVARCHAR(255) NOT NULL)");
}

var people = new List<Person>
{
    new Person { FullName = new FullName { FirstName = "A", LastName = "B" }}, 
    new Person { FullName = new FullName { FirstName = "C", LastName = "D"}}
};

await using var sqlConnection = new SqlConnection(connectionString);
{
    sqlConnection.BulkInsert(people);
}

await using var sqlConnectionQuery = new SqlConnection(connectionString);
{
    var result = await sqlConnectionQuery.QueryAsync<Person>("SELECT * FROM [Person]");
    foreach (var person in result)
    {
        Console.WriteLine($"IdentityId : {person.IdentityId} FirstName : {person.FullName.FirstName} LastName : {person.FullName.LastName}");
    }
}


Console.ReadKey();
public class Person
{
    public int IdentityId { get; set; }
    
    public FullName FullName { get; set; }
}

public class FullName
{
    public string FirstName { get; set; }
    public string LastName { get; set; }
}

public class JsonTypeHandler<T> : SqlMapper.TypeHandler<T>
{
    public override void SetValue(IDbDataParameter parameter, T value)
    {
        parameter.Value = JsonSerializer.Serialize(value);
    }

    public override T Parse(object value)
    {
        return value is null ? default : JsonSerializer.Deserialize<T>(value.ToString());
    }
}