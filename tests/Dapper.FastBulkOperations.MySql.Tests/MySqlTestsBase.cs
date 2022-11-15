using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using MySqlConnector;

namespace Dapper.FastBulkOperations.MySql.Tests;

public class MySqlGuidTypeHandler : SqlMapper.TypeHandler<Guid>
{
	public override void SetValue(IDbDataParameter parameter, Guid guid)
	{
		parameter.Value = guid.ToString();
	}

	public override Guid Parse(object value)
	{
		return new Guid((string)value);
	}
}

public class  AllFieldTypesWithIdentityTests
{
	public long Id { get; set;}
	public string NvarcharValue { get; set; }
	public EnumValue EnumValue { get; set;}
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

public enum EnumValue : short
{
	First = 0,
	Second,
	Third
}

public class MySqlTestsBase
{
	protected MySqlTestsBase()
	{
		SqlMapper.AddTypeHandler(new MySqlGuidTypeHandler());
		SqlMapper.RemoveTypeMap(typeof(Guid));
		SqlMapper.RemoveTypeMap(typeof(Guid?));
	}
	
	protected readonly string BigText = string.Join(string.Empty, Enumerable.Range(0, 1000).Select(x => x.ToString()));
	protected readonly DateTime DateTime = new DateTime(2022, 1, 1);
	protected readonly string XmlValue = "<root><text>test</text></root>";
    protected static readonly string ConnectionString = "Server=localhost;Database=tempdb;Uid=root;Pwd=1;Port=13306;AllowLoadLocalInfile=true;Allow User Variables=true";

    protected void DropTable(string name)
    {
	    using var connection = new MySqlConnection(ConnectionString);
	    connection.Open();

	    connection.Execute(GetDropTableQuery(name));
    }
    protected void CreateCustomPrimaryKeysTableWithNotMapped(string name, bool createNotMappedField = true)
    {
	    using var connection = new MySqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"{GetDropTableQuery(name)}
                CREATE TABLE `{name}`
                (
                    `FirstKey` varchar(36) NOT NULL,
                    `SecondKey` varchar(36) NOT NULL,
					`FieldToUpdate` varchar(100),
					{(createNotMappedField ? "`Exclude` varchar(36)," : string.Empty )}
					CONSTRAINT pk_{name}  PRIMARY KEY (`FirstKey`, `SecondKey`)
                );");
	    }
    }
    protected void CreateAttributeBasedMappingTableWithNotMapped(string name, bool createNotMappedField = true)
    {
	    using var connection = new MySqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"{GetDropTableQuery(name)}
                CREATE TABLE `{name}`
                (
                    `FirstKey` varchar(36) NOT NULL,
                    `SecondKey` varchar(36) NOT NULL,
					`Field` varchar(100),
					{(createNotMappedField ? "`NotMapped` varchar(36)," : string.Empty )}
					CONSTRAINT pk_{name}  PRIMARY KEY (`FirstKey`, `SecondKey`)
                );");
	    }
    }
    
    protected void CreateAllFieldsTable(string name)
    {
	    using var connection = new MySqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute($@"{GetDropTableQuery(name)}
                CREATE TABLE `{name}`
                (
                    `Id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    `NvarcharValue` VARCHAR(100) NULL,
					`EnumValue` smallint NULL,
					`BigTextValue` text,
					`IntValue` integer NULL,
					`DecimalValue` DECIMAL(18, 6) NULL,
                    `GuidValue` varchar(36) NULL,
                    `CreateDate` date NULL)");
	    }
    }
    
    protected void InsertAttributeBasedMapping(string tableName, AttributeBasedMapping fields, bool includeNotMapped)
    {
	    using var connection = new MySqlConnection(ConnectionString);
	    {
		    connection.Open(); 
		    connection.Execute(
			    $@"
                INSERT INTO `{tableName}`
                (
                    `FirstKey`,
					`SecondKey`,
					`Field`
					{(includeNotMapped ? ",`NotMapped`" : string.Empty)}
                ) VALUES(@FirstKey, @SecondKey, @FieldToUpdate {(includeNotMapped ? ",@NotMapped" : string.Empty)})
                ", param:fields);
	    }
    }

    protected void InsertCustomPrimaryKeyFieldsParametersMappings(string tableName, CustomPrimaryKeyFieldsParametersMappings fields, bool includeExclude)
    {
	    using var connection = new MySqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"
                INSERT INTO `{tableName}`
                (
                    `FirstKey`,
					`SecondKey`,
					`FieldToUpdate`
					{(includeExclude ? ",`Exclude`" : string.Empty)}
                ) VALUES(@FirstKey, @SecondKey, @FieldToUpdate {(includeExclude ? ",@Exclude" : string.Empty)})
                ", param:fields);
	    }
    }
    
    protected long InsertAllFields(string tableName, AllFieldTypesWithIdentityTests fields)
    {
	    try
	    {
		    using var connection = new MySqlConnection(ConnectionString);
		    {
			    connection.Open();
			    var str =  $@"
                INSERT INTO `{tableName}`
                (
                    `NvarcharValue`,
					`EnumValue`,
					`BigTextValue`,
					`IntValue`,
					`DecimalValue`,
                    `GuidValue`,
                    `CreateDate`
                ) VALUES(@NvarcharValue, @EnumValue, @BigTextValue, @IntValue, @DecimalValue, @GuidValue, @CreateDate);
                
                SELECT LAST_INSERT_ID();";
			    return connection.QueryFirst<long>(str
				    , param:fields);
		    }
	    }
	    catch (Exception e)
	    {
		    ;
	    }

	    return 0L;
    }
    
    protected void AllFieldsTestAssertions(IEnumerable<AllFieldTypesWithIdentityTests> select, 
	    IEnumerable<AllFieldTypesWithIdentityTests> items)
    {
	    var count = select.Count();
	    Assert.Equal(count, items.Count());
	    Assert.True(select.All(x => x.EnumValue == EnumValue.Second));
	    Assert.True(select.Select(x => x.Id).SequenceEqual(items.Select(x => x.Id)));
	    Assert.True(select.All(x => x.BigTextValue == BigText));
	    Assert.True(select.All(x => x.CreateDate == DateTime));
	    Assert.True(select.OrderBy(x => x.IntValue).Select(x => x.IntValue).SequenceEqual(Enumerable.Range(0, count)));
	    Assert.True(select.OrderBy(x => x.DecimalValue).Select(x => x.DecimalValue).SequenceEqual(Enumerable.Range(0, count).Select(x => (decimal)x)));
	    Assert.True(select.OrderBy(x => x.NvarcharValue).Select(x => x.NvarcharValue).SequenceEqual(Enumerable.Range(0, count).Select(x => $"Test {x}").OrderBy(x => x)));
	    Assert.True(select.All(x => x.GuidValue != Guid.Empty));
    }
    
    private string GetDropTableQuery(string name) =>
	    $"DROP TABLE IF EXISTS `{name}`;";

}
        
        