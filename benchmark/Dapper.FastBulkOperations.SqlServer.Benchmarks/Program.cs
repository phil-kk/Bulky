using BenchmarkDotNet.Running;
using Dapper.FastBulkOperations.SqlServer.Benchmarks;

BenchmarkRunner.Run<MySqlMergeBenchmarks>();