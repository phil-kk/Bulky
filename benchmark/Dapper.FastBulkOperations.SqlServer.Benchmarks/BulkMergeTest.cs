using System.ComponentModel.DataAnnotations.Schema;

namespace Dapper.FastBulkOperations.SqlServer.Benchmarks;

[Table("BulkMergeTest")]
internal class BulkMergeTest
{
    [Column("Id")]
    public int Id { get; set; }
    public string Name { get; set; }
    public decimal Amount { get; set; }
    public TestInt Enum { get; set; }
}