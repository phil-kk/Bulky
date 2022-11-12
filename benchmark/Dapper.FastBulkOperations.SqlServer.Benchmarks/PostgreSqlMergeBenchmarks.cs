using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Dapper.FastBulkOperations.SqlServer.Benchmarks;
using Z.Dapper.Plus;



namespace Dapper.FastBulkOperations.SqlServer.Benchmarks;
internal class BulkPgMergeTest
{
    public long Id { get; set; }
    public string TestVarchar { get; set; }
    public int TestInt { get; set; }
}

public class TestPks
{
    public Guid FirstKey { get; set; }
	
    public Guid SecondKey { get; set; }
    public string FieldToUpdate { get; set; }
}
[SimpleJob(RunStrategy.Monitoring)]
[AllStatisticsColumn]
[Config(typeof(AntiVirusFriendlyConfig))]
[MemoryDiagnoser(displayGenColumns:true)]
public class PostgreSqlMergeBenchmarks
{
    private readonly List<TestPks> _list = new List<TestPks>();
    [GlobalSetup]
    public void GlobalSetup()
    {
        for (var i = 0; i < 100000; i++)
        {
            _list.Add(new TestPks { FirstKey = Guid.NewGuid(), SecondKey = Guid.NewGuid(), FieldToUpdate = "test"});
        }
        
    }
    
    [Benchmark]
    public void BulkExtensions()
    {
        using var connection =
            new Npgsql.NpgsqlConnection("Server=localhost; Port=5432; User Id=postgres; Password=1; Database=tempdb");
        Dapper.FastBulkOperations.PostgreSql.NpgsqlBulkExtensions.BulkInsertOrUpdate(connection, _list);
    }
    
    [Benchmark]
    public void DapperPlus()
    {
        using var connection =
            new Npgsql.NpgsqlConnection("Server=localhost; Port=5432; User Id=postgres; Password=1; Database=tempdb");
        Z.Dapper.Plus.DapperPlusExtensions.BulkMerge(connection, _list);
    }
}