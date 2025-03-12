using BenchmarkDotNet.Attributes;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Npgsql;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Xml.Linq;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using Bulky.PostgreSql;
using Z.Dapper.Plus;
using Z.Dapper;
using PostgreSQL.Bulk;
using System.Data;
namespace Bulky.Benchmarks;

public class AllFieldTypesWithIdentityTests
{
    public long Id { get; set; }
    public string NvarcharValue { get; set; }
    public EnumValue? EnumValue { get; set; }
    public string BigTextValue { get; set; }
    public int IntValue { get; set; }
    public decimal? DecimalValue { get; set; }
    public Guid GuidValue { get; set; }
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
public class Default
{
    protected static string ConnectionString = "Server=localhost; Port=5432; User Id=postgres; Password=1; Database=tempdb";
    private readonly List<AllFieldTypesWithIdentityTests> List;
    private string GetDropTableQuery(string name) =>
        $"DROP TABLE IF EXISTS \"{name}\";";
    public Default()
    {
        List = Generate(100000); 
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        connection.Execute($@"{GetDropTableQuery(nameof(AllFieldTypesWithIdentityTests))}
                CREATE TABLE ""{nameof(AllFieldTypesWithIdentityTests)}""
                (
                    ""Id"" bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1) PRIMARY KEY,
                    ""NvarcharValue"" character varying(100) NULL,
					""EnumValue"" smallint NULL,
					""BigTextValue"" text COLLATE pg_catalog.""default"",
					""IntValue"" integer NULL,
					""DecimalValue"" decimal NULL,
                    ""GuidValue"" uuid NULL,
                    ""CreateDate"" date NULL)");


        using var c = new NpgsqlConnection(ConnectionString);
        c.Open();
        c.BulkInsert(List.Take(1).ToList(), nameof(AllFieldTypesWithIdentityTests));
        DapperPlusManager
                .Entity<AllFieldTypesWithIdentityTests>()
                .Table(nameof(AllFieldTypesWithIdentityTests))
                .Identity(x => x.Id, true);
    }
    List<AllFieldTypesWithIdentityTests> Generate(int count = 10_000)
    {
        return Enumerable.Range(0, count).Select(x => new AllFieldTypesWithIdentityTests
        {
            BigTextValue = $"TextTextText {x}",
            CreateDate = DateTime.Now.AddDays(x),
            DecimalValue = x,
            EnumValue = null,
            GuidValue = Guid.NewGuid(),
            IntValue = x,
            NvarcharValue = $"Test {x}"
        }).ToList();
    }

    [Benchmark]
    public async Task Bulky()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        await connection.BulkInsertAsync(List, nameof(AllFieldTypesWithIdentityTests));
    }

    [Benchmark]
    public async Task ZDapper()
    {
        using var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        await (connection as IDbConnection).BulkInsertAsync(List);
    }
}