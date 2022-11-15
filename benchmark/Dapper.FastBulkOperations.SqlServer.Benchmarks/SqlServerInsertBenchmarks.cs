using System.Data.SqlClient;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Z.Dapper.Plus;

namespace Dapper.FastBulkOperations.SqlServer.Benchmarks;

[SimpleJob(RunStrategy.Monitoring)]
[AllStatisticsColumn]
[Config(typeof(AntiVirusFriendlyConfig))]
[MemoryDiagnoser(displayGenColumns:true)]
public class SqlServerInsertBenchmarks
{
    private readonly IList<BulkMergeTest> _list = new List<BulkMergeTest>();
    [GlobalSetup]
    public void GlobalSetup()
    {
        DapperPlusManager.Entity<BulkMergeTest>()
            .Identity(x => x.Id);
        using var connection = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;");
        connection.Execute("TRUNCATE TABLE BulkMergeTest");
        for (var i = 0; i < 100000; i++)
        {
            _list.Add(new BulkMergeTest {TestVarchar = $"Test{1}"});
        }
    }
    
    [Benchmark]
    public void FastBulkOperations()
    {
        using var connection = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;");
        SqlServerBulkExtensions.BulkInsert(connection, _list);
    }
    
    [Benchmark]
    public void DapperPlus()
    {
        using var connection = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;");
        Z.Dapper.Plus.DapperPlusExtensions.BulkInsert(connection, _list);
    }
    
    [Benchmark]
    public void _DapperLike()
    {
        using var connection = new SqlConnection("Server=localhost;Database=DapperTest;Trusted_Connection=True;");
        DapperLike.SqlBulkCopyExtensions.BulkInsert(connection, _list);
    }
}