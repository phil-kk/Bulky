using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Toolchains.InProcess.NoEmit;

namespace Dapper.FastBulkOperations.SqlServer.Benchmarks;

public class AntiVirusFriendlyConfig : ManualConfig
{
    public AntiVirusFriendlyConfig()
    {
        AddJob(Job.ShortRun
            .WithToolchain(InProcessNoEmitToolchain.Instance));
    }
}