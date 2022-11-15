
using Dapper;
using Dapper.FastBulkOperations.MySql;
using Dapper.FastBulkOperations.SqlServer;
using MySqlConnector; // or System.Data.SqlClient

const string connectionString = "Server=localhost;Database=tempdb;Uid=root;Pwd=1;Port=13306;AllowLoadLocalInfile=true;Allow User Variables=true";

await using var create = new MySqlConnection(connectionString);
{
    create.Execute("CREATE TABLE IF NOT EXISTS `Person` (`IdentityId` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `FullName` NVARCHAR(255) NOT NULL, `intTest` INT)");
}

var people = new List<Person> { new Person { FullName = "Filipp Kleymenov"}, new Person { FullName = "John Ellison ", intTest = 1}};

await using var sqlConnection = new MySqlConnection(connectionString);
sqlConnection.BulkUpdate(people);

foreach (var person in people)
{
    Console.WriteLine($"IdentityId : {person.IdentityId} FullName : {person.FullName}");
}

Console.ReadKey();
public class Person
{
    public int IdentityId { get; set; }
    
    public string FullName { get; set; }
    
    public int intTest { get; set; }
}