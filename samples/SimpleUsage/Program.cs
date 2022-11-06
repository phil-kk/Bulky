
using Dapper;
using Dapper.FastBulkOperations.SqlServer;
using Microsoft.Data.SqlClient;

const string connectionString = "Server=localhost;Database=tempdb;Trusted_Connection=True;TrustServerCertificate=true;";

await using var create = new SqlConnection(connectionString);
{
    create.Execute($"IF OBJECT_ID('Person', 'U') IS NOT NULL DROP TABLE [Person]");
    create.Execute("CREATE TABLE [Person] ([Id] INT NOT NULL IDENTITY(1,1) PRIMARY KEY, [FullName] NVARCHAR(255) NOT NULL)");
}

var people = new List<Person> { new Person { FullName = "Filipp Kleymenov"}, new Person { FullName = "John Ellison "}};

await using var sqlConnection = new SqlConnection(connectionString);
sqlConnection.BulkInsert(people);

foreach (var person in people)
{
    Console.WriteLine($"Id : {person.Id} FullName : {person.FullName}");
}

Console.ReadKey();
public class Person
{
    public int Id { get; set; }
    
    public string FullName { get; set; }
}