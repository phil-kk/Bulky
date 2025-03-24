namespace BulkyMerge.SqlServer;

public static partial class SqlServerBulkExtensions
{
    private static readonly SqlServerDialect Dialect = new();
    private static readonly SqlServerBulkWriter BulkWriter = new(Dialect);
}