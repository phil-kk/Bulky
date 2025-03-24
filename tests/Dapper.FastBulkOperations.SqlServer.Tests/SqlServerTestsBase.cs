using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Dapper;
using Microsoft.Data.SqlClient;

namespace BulkyMerge.SqlServer.Tests;

public class  AllFieldTypesWithIdentityTests
{
	public long Id { get; set;}
	public string NvarcharValue { get; set; }
	public EnumValue EnumValue { get; set;}
	public string XmlValue { get; set; }
	public string BigTextValue { get; set; }
	public int IntValue { get; set; }
	public decimal DecimalValue { get; set;}
	public Guid GuidValue { get; set;}
	public DateTime CreateDate { get; set; }
}

[Table("AttributeBasedMapping_Test")]
public class AttributeBasedMapping
{
	[Key]
	public Guid FirstKey { get; set; }
	
	[Key] 
	public Guid SecondKey { get; set; }
	
	[Column("Field")]
	public string FieldToUpdate { get; set; }
	
	[NotMapped]
	public string NotMapped { get; set; }
}

public class CustomPrimaryKeyFieldsParametersMappings
{
	public Guid FirstKey { get; set; }
	
	public Guid SecondKey { get; set; }
	
	public string FieldToUpdate { get; set; }
	public string Exclude { get; set; }
}

public enum EnumValue
{
	First = 0,
	Second,
	Third
}

public class SqlServerTestsBase
{
	protected readonly string BigText = string.Join(string.Empty, Enumerable.Range(0, 1000).Select(x => x.ToString()));
	protected readonly DateTime DateTime = new DateTime(2022, 1, 1);
	protected readonly string XmlValue = "<root><text>test</text></root>"; 
	protected static string ConnectionString = "Server=localhost,1433;Database=master;User Id=sa;Password=YourStrong!Passw0rd;TrustServerCertificate=True;";

    protected void DropTable(string name)
    {
	    using var connection = new SqlConnection(ConnectionString);
	    connection.Open();

	    connection.Execute(GetDropTableQuery(name));
    }
    protected void CreateCustomPrimaryKeysTableWithNotMapped(string name, bool createNotMappedField = true)
    {
	    using var connection = new SqlConnection(ConnectionString);
	    {
		    connection.Open();
            connection.Execute(
			    $@"{GetDropTableQuery(name)}
                CREATE TABLE {name}
                (
                    [FirstKey] UNIQUEIDENTIFIER NOT NULL,
                    [SecondKey] UNIQUEIDENTIFIER NOT NULL,
					[FieldToUpdate] NVARCHAR(100),
					{(createNotMappedField ? "[Exclude] NVARCHAR(100) DEFAULT('')," : string.Empty )}
					CONSTRAINT pk_{name}  PRIMARY KEY ([FirstKey], [SecondKey])
                );");
	    }
    }
    protected void CreateAttributeBasedMappingTableWithNotMapped(string name, bool createNotMappedField = true)
    {
	    using var connection = new SqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"{GetDropTableQuery(name)}
                CREATE TABLE {name}
                (
                    [FirstKey] UNIQUEIDENTIFIER NOT NULL,
                    [SecondKey] UNIQUEIDENTIFIER NOT NULL,
					[Field] NVARCHAR(100),
					{(createNotMappedField ? "[NotMapped] NVARCHAR(100) DEFAULT('')," : string.Empty )}
					CONSTRAINT pk_{name}  PRIMARY KEY ([FirstKey], [SecondKey])
                );");
	    }
    }
    
    protected void CreateAllFieldsTable(string name)
    {
	    using var connection = new SqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"{GetDropTableQuery(name)}
                CREATE TABLE {name}
                (
                    [Id] BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    [NvarcharValue] NVARCHAR(100),
					[EnumValue] TINYINT,
					[XmlValue] XML,
					[BigTextValue] NVARCHAR(MAX),
					[IntValue] INT,
					[DecimalValue] DECIMAL(18, 6),
                    [GuidValue] UNIQUEIDENTIFIER NULL,
                    [CreateDate] DATETIME2
                );");
	    }
    }
    
    protected void InsertAttributeBasedMapping(string tableName, AttributeBasedMapping fields, bool includeNotMapped)
    {
	    using var connection = new SqlConnection(ConnectionString);
	    {
		    connection.Open(); 
		    connection.Execute(
			    $@"
                INSERT INTO {tableName}
                (
                    [FirstKey],
					[SecondKey],
					[Field]
					{(includeNotMapped ? ",[NotMapped]" : String.Empty)}
                ) VALUES(@FirstKey, @SecondKey, @FieldToUpdate {(includeNotMapped ? ",@NotMapped" : String.Empty)})
                ", param:fields);
	    }
    }

    protected void InsertCustomPrimaryKeyFieldsParametersMappings(string tableName, CustomPrimaryKeyFieldsParametersMappings fields, bool includeExclude)
    {
	    using var connection = new SqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"
                INSERT INTO {tableName}
                (
                    [FirstKey],
					[SecondKey],
					[FieldToUpdate]
					{(includeExclude ? ",[Exclude]" : String.Empty)}
                ) VALUES(@FirstKey, @SecondKey, @FieldToUpdate {(includeExclude ? ",@Exclude" : String.Empty)})
                ", param:fields);
	    }
    }
    
    protected long InsertAllFields(string tableName, AllFieldTypesWithIdentityTests fields)
    {
	    using var connection = new SqlConnection(ConnectionString);
	    {
		    connection.Open();
		    return connection.QueryFirst<long>(
			    $@"
                INSERT INTO {tableName}
                (
                    [NvarcharValue],
					[EnumValue],
					[XmlValue],
					[BigTextValue],
					[IntValue],
					[DecimalValue],
                    [GuidValue],
                    [CreateDate]
                ) VALUES(@NvarcharValue, @EnumValue, @XmlValue, @BigTextValue, @IntValue, @DecimalValue, @GuidValue, @CreateDate)
                
                SELECT SCOPE_IDENTITY()", param:fields);
	    }
    }
    
    protected void AllFieldsTestAssertions(IEnumerable<AllFieldTypesWithIdentityTests> select, 
	    IEnumerable<AllFieldTypesWithIdentityTests> items)
    {
	    var count = select.Count();
	    Assert.Equal(count, items.Count());
	    Assert.True(select.All(x => x.EnumValue == EnumValue.Second));
	    Assert.True(select.Select(x => x.Id).SequenceEqual(items.Select(x => x.Id)));
	    Assert.True(select.All(x => x.XmlValue == XmlValue));
	    Assert.True(select.All(x => x.BigTextValue == BigText));
	    Assert.True(select.All(x => x.CreateDate == DateTime));
	    Assert.True(select.OrderBy(x => x.IntValue).Select(x => x.IntValue).SequenceEqual(Enumerable.Range(0, count)));
	    Assert.True(select.OrderBy(x => x.DecimalValue).Select(x => x.DecimalValue).SequenceEqual(Enumerable.Range(0, count).Select(x => (decimal)x)));
	    Assert.True(select.OrderBy(x => x.NvarcharValue).Select(x => x.NvarcharValue).SequenceEqual(Enumerable.Range(0, count).Select(x => $"Test {x}").OrderBy(x => x)));
	    Assert.True(select.All(x => x.GuidValue != Guid.Empty));
    }
    
    private string GetDropTableQuery(string name) =>
	    $"IF OBJECT_ID('{name}', 'U') IS NOT NULL DROP TABLE {name};";

}
        
        