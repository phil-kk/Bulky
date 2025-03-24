using BulkyMerge.Root;
using BulkyMerge.SqlServer;
using Dapper;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Data;
using static Dapper.SqlMapper;
const string connectionString = "Server=localhost,1433;Database=master;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=True;";

await using var create = new SqlConnection(connectionString);
{
    create.Execute($"IF OBJECT_ID('Person', 'U') IS NOT NULL DROP TABLE [Person]");
    create.Execute("CREATE TABLE [Person] ([IdentityId] INT NOT NULL IDENTITY(1,1) PRIMARY KEY, [FullName] NVARCHAR(255) NOT NULL, [JsonObj] NVARCHAR(MAX) NOT NULL)");
}

TypeConverters.RegisterTypeConverter(typeof(JsonObj), Newtonsoft.Json.JsonConvert.SerializeObject);

var people = new List<Person> { new Person { FullName = "A B", JsonObj = new JsonObj { JsonProp = "test" } }, new Person { FullName = "C D", JsonObj = new JsonObj { JsonProp = "test2" } } };

await using var sqlConnection = new SqlConnection(connectionString); // MysqlConenction or NpgsqlConnection
sqlConnection.BulkInsert(people);


foreach (var person in people)
{
    Console.WriteLine($"IdentityId : {person.IdentityId} FullName : {person.FullName}");
}


SqlMapper.AddTypeHandler(typeof(JsonObj), new DappetTypeHandler());
await using var fetchConnection = new SqlConnection(connectionString);
var result = await fetchConnection.QueryAsync<Person>("SELECT * FROM [Person]");

foreach (var p in result)
{
    Console.WriteLine(p.JsonObj);
    ;
}
// No need in any Mapping code, Primary Keys and Identity will be found automatically
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

class DappetTypeHandler : ITypeHandler
{
    public object Parse(Type destinationType, object value)
    {
        return JsonConvert.DeserializeObject(value.ToString(), destinationType);
    }

    public void SetValue(IDbDataParameter parameter, object value)
    {
        throw new NotImplementedException();
    }
}
