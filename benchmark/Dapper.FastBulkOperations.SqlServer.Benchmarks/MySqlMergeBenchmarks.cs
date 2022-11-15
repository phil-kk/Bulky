using System.ComponentModel.DataAnnotations.Schema;
using System.Data.SqlClient;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Dapper.FastBulkOperations.SqlServer.Benchmarks;
using MySqlConnector;
using Npgsql;
using Z.Dapper.Plus;



namespace Dapper.FastBulkOperations.SqlServer.Benchmarks;
internal class BulkMyMergeTest
{
    public long Id { get; set; }
    public string TestVarchar { get; set; }
    public int TestInt { get; set; }
}

public class TestMyPks
{
    public Guid FirstKey { get; set; }
	
    public Guid SecondKey { get; set; }
    public string FieldToUpdate { get; set; }
}
[SimpleJob(RunStrategy.Monitoring)]
[AllStatisticsColumn]
[Config(typeof(AntiVirusFriendlyConfig))]
[MemoryDiagnoser(displayGenColumns:true)]
public class MySqlMergeBenchmarks
{
    private readonly List<BulkMyMergeTest> _list = new List<BulkMyMergeTest>();
    [GlobalSetup]
    public void GlobalSetup()
    {
        DapperPlusManager.Entity<BulkPgMergeTest>()
            .Identity(x => x.Id);
        var temp = new List<BulkMyMergeTest>();
        for (var i = 0; i < 50000; i++)
        {
            temp.Add(new BulkMyMergeTest {  TestVarchar = $"test{10 + i}"});
        }
        for (var i = 0; i < 50000; i++)
        {
            _list.Add(new BulkMyMergeTest {  TestVarchar = $"test{10 + i}"});
        }
        using var connection = new MySqlConnection("Server=localhost;Database=tempdb;Uid=root;Pwd=1;Port=13306;AllowLoadLocalInfile=true;Allow User Variables=true");
        {
            Dapper.FastBulkOperations.MySql.MySqlBulkExtensions.BulkInsertOrUpdate(connection, temp);
        }
        _list.AddRange(temp);
        
    }
    
    [Benchmark]
    public void BulkExtensions()
    {
        using var connection =
            new MySqlConnection("Server=localhost;Database=tempdb;Uid=root;Pwd=1;Port=13306;AllowLoadLocalInfile=true;Allow User Variables=true");
        Dapper.FastBulkOperations.MySql.MySqlBulkExtensions.BulkInsertOrUpdate(connection, _list);
    }
    
    [Benchmark]
    public void DapperPlus()
    {
        using var connection =
            new MySqlConnection("Server=localhost;Database=tempdb;Uid=root;Pwd=1;Port=13306;AllowLoadLocalInfile=true;Allow User Variables=true");
        Z.Dapper.Plus.DapperPlusExtensions.BulkMerge(connection, _list);
    }
}