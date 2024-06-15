using BenchmarkDotNet.Attributes;

namespace Dapper.FastBulkOperations.Benchmarks;

public class Default
{
    // And define a method with the Benchmark attribute
    [Benchmark]
    public void Sleep() => Thread.Sleep(10);

    // You can write a description for your method.
    [Benchmark(Description = "Thread.Sleep(10)")]
    public void SleepWithDescription() => Thread.Sleep(10);
}