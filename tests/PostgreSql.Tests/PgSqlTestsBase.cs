using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using BulkyMerge.Root;
using Dapper;
using Npgsql;

namespace BulkyMerge.PostgreSql.Tests;
/*
public class  AllFieldTypesWithIdentityTests
{
    public long Id { get; set; }
    public string VarcharValue { get; set; }
    public string TextValue { get; set; }
    public char CharValue { get; set; }
    public bool BooleanValue { get; set; }
    public short SmallIntValue { get; set; }
    public int IntValue { get; set; }
    public long BigIntValue { get; set; }
    public decimal DecimalValue { get; set; }
    public float RealValue { get; set; }
    public double DoublePrecisionValue { get; set; }

    // Даты и время
    public DateTime? DateValue { get; set; }  // date
    public TimeSpan? TimeValue { get; set; }  // time
    public DateTime TimestampValue { get; set; }
    public DateTimeOffset? TimestampTzValue { get; set; }  // timestamptz

    public Guid UuidValue { get; set; }
    public string JsonValue { get; set; }
    public byte[] ByteaValue { get; set; }

    public List<JsontTest> JsonbValue { get; set; }
}
*/
public class AllFieldTypesWithIdentityTests
{
    public long Id { get; set; }
    public string NvarcharValue { get; set; }
    public EnumValue? EnumValue { get; set; }
    public string BigTextValue { get; set; }
    public int IntValue { get; set; }
    public decimal DecimalValue { get; set; }
    public Guid GuidValue { get; set; }
    public DateTime CreateDate { get; set; }
}

public class JsontTest
{ 
    public string Test { get; set; }
    public int Test2 { get; set; }
    public decimal Decimal2 { get; set; }
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

public class PgSqlTestsBase
{
	protected readonly string BigText = string.Join(string.Empty, Enumerable.Range(0, 1000).Select(x => x.ToString()));
	protected readonly DateTime DateTime = new DateTime(2022, 1, 1);
	protected readonly string XmlValue = "<root><text>test</text></root>";
    protected static string ConnectionString = "Host=localhost;Port=5432;Database=postgres;Username=postgres;Password=YourPassword;";

    protected void DropTable(string name)
    {
	    using var connection = new NpgsqlConnection(ConnectionString);
	    connection.Open();

	    connection.Execute(GetDropTableQuery(name));
    }
    protected void CreateCustomPrimaryKeysTableWithNotMapped(string name, bool createNotMappedField = true)
    {
	    using var connection = new NpgsqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"{GetDropTableQuery(name)}
                CREATE TABLE ""{name}""
                (
                    ""FirstKey"" uuid NOT NULL,
                    ""SecondKey"" uuid NOT NULL,
					""FieldToUpdate"" character varying(100),
					{(createNotMappedField ? "\"Exclude\" character varying(100)," : string.Empty )}
					CONSTRAINT pk_{name}  PRIMARY KEY (""FirstKey"", ""SecondKey"")
                );");
	    }
    }
    protected void CreateAttributeBasedMappingTableWithNotMapped(string name, bool createNotMappedField = true)
    {
	    using var connection = new NpgsqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"{GetDropTableQuery(name)}
                CREATE TABLE ""{name}""
                (
                    ""FirstKey"" uuid NOT NULL,
                    ""SecondKey"" uuid NOT NULL,
					""Field"" character varying(100),
					{(createNotMappedField ? "\"NotMapped\" character varying(100)," : string.Empty )}
					CONSTRAINT pk_{name}  PRIMARY KEY (""FirstKey"", ""SecondKey"")
                );");
	    }
    }

    /*
     * public long Id { get; set; }
    public string NvarcharValue { get; set; }
    public EnumValue EnumValue { get; set; }
    public string BigTextValue { get; set; }
    public int IntValue { get; set; }
    public decimal DecimalValue { get; set; }
    public Guid GuidValue { get; set; }
    public DateTime CreateDate { get; set; }
     * */

    protected void CreateAllFieldsTable(string name)
    {
       /* TypeConverters.RegisterTypeConverter(typeof(EnumValue), (e) =>
        {
            return Convert.ToInt32(e);
        });*/
        using var connection = new NpgsqlConnection(ConnectionString);
        {
            connection.Open();
            connection.Execute($@"
            {GetDropTableQuery(name)}
            CREATE TABLE ""{name}""
            (
                ""Id"" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                ""IntValue"" integer NULL,
                ""BigIntValue"" bigint NULL,
                ""DecimalValue"" decimal(10, 4) NULL,
                ""NvarcharValue"" varchar(255) NULL,
                ""EnumValue"" integer NULL,
                ""BigTextValue"" TEXT NULL,
                ""CreateDate"" date NULL,
                ""GuidValue"" uuid NULL
            )");
        }
    }


    protected void InsertAttributeBasedMapping(string tableName, AttributeBasedMapping fields, bool includeNotMapped)
    {
	    using var connection = new NpgsqlConnection(ConnectionString);
	    {
		    connection.Open(); 
		    connection.Execute(
			    $@"
                INSERT INTO ""{tableName}""
                (
                    ""FirstKey"",
					""SecondKey"",
					""Field""
					{(includeNotMapped ? ",\"NotMapped\"" : string.Empty)}
                ) VALUES(@FirstKey, @SecondKey, @FieldToUpdate {(includeNotMapped ? ",@NotMapped" : string.Empty)})
                ", param:fields);
	    }
    }

    protected void InsertCustomPrimaryKeyFieldsParametersMappings(string tableName, CustomPrimaryKeyFieldsParametersMappings fields, bool includeExclude)
    {
	    using var connection = new NpgsqlConnection(ConnectionString);
	    {
		    connection.Open();
		    connection.Execute(
			    $@"
                INSERT INTO ""{tableName}""
                (
                    ""FirstKey"",
					""SecondKey"",
					""FieldToUpdate""
					{(includeExclude ? ",\"Exclude\"" : string.Empty)}
                ) VALUES(@FirstKey, @SecondKey, @FieldToUpdate {(includeExclude ? ",@Exclude" : string.Empty)})
                ", param:fields);
	    }
    }
    
    protected long InsertAllFields(string tableName, AllFieldTypesWithIdentityTests fields)
    {
	    using var connection = new NpgsqlConnection(ConnectionString);
	    {
		    connection.Open();
		    return connection.QueryFirst<long>(
			    $@"
                INSERT INTO ""{tableName}""
                (
                    ""NvarcharValue"",
					""EnumValue"",
					""BigTextValue"",
					""IntValue"",
					""DecimalValue"",
                    ""GuidValue"",
                    ""CreateDate""
                ) VALUES(@NvarcharValue, @EnumValue, @BigTextValue, @IntValue, @DecimalValue, @GuidValue, @CreateDate);
                
                SELECT currval(pg_get_serial_sequence('""{tableName}""','Id'));", param:fields);
	    }
    }
    
    protected void AllFieldsTestAssertions(IEnumerable<AllFieldTypesWithIdentityTests> select, 
	    IEnumerable<AllFieldTypesWithIdentityTests> items)
    {
	    var count = select.Count();
	    Assert.Equal(count, items.Count());
	    Assert.True(select.Select(x => x.Id).SequenceEqual(items.Select(x => x.Id)));
	    Assert.True(select.OrderBy(x => x.IntValue).Select(x => x.IntValue).SequenceEqual(Enumerable.Range(0, count)));
	    Assert.True(select.OrderBy(x => x.DecimalValue).Select(x => x.DecimalValue).SequenceEqual(Enumerable.Range(0, count).Select(x => (decimal)x)));
        Assert.True(select.All(x => x.EnumValue == EnumValue.Third));
    }
    
    private string GetDropTableQuery(string name) =>
	    $"DROP TABLE IF EXISTS \"{name}\";";

}
        
        