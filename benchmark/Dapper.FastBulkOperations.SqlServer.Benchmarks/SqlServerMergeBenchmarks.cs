using System.Data.SqlClient;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Z.Dapper.Plus;

namespace Dapper.FastBulkOperations.SqlServer.Benchmarks;

[SimpleJob(RunStrategy.Monitoring)]
[AllStatisticsColumn]
[Config(typeof(AntiVirusFriendlyConfig))]
[MemoryDiagnoser(displayGenColumns:true)]
public class SqlServerMergeBenchmarks
{
    private readonly List<BulkMergeTest> _list = new List<BulkMergeTest>();
    [GlobalSetup]
    public void GlobalSetup()
    {
        using var t = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;TrustServerCertificate=true;");
        {
            t.Execute("TRUNCATE TABLE BulkMergeTest");
        }
        DapperPlusManager.Entity<BulkMergeTest>()
            .Identity(x => x.Id);
        var temp = new List<BulkMergeTest>();
        for (var i = 0; i < 50000; i++)
        {
            temp.Add(new BulkMergeTest {  TestVarchar = $"test{10 + i}"});
        }
        for (var i = 0; i < 50000; i++)
        {
            _list.Add(new BulkMergeTest {   TestVarchar = $"test{10 + i}"});
        }
        using var connection = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;");
        {
            SqlServerBulkExtensions.BulkInsert(connection, temp);
        }
        _list.AddRange(temp);
        
    }
    
    [Benchmark]
    public void FastBulkOperations()
    {
        using var connection = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;TrustServerCertificate=true;");
        SqlServerBulkExtensions.BulkInsertOrUpdate(connection, _list);
    }
    
    [Benchmark]
    public void DapperPlus()
    {
        using var connection = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;TrustServerCertificate=true;");
        Z.Dapper.Plus.DapperPlusExtensions.BulkMerge(connection, _list);
    }
}