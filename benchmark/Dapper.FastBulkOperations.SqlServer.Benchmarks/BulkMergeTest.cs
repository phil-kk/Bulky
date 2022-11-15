using System.ComponentModel.DataAnnotations.Schema;

namespace Dapper.FastBulkOperations.SqlServer.Benchmarks;

[Table("BulkMergeTest")]
internal class BulkMergeTest
{
    [Column("Id")]
    public int Id { get; set; }
    public string TestVarchar { get; set; }
}