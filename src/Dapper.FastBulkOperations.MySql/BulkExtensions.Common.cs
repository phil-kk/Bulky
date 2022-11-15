namespace Dapper.FastBulkOperations.MySql;

public static partial class MySqlBulkExtensions
{
    private static readonly MySqlDialect Dialect = new();
    private static readonly MySqlBulkWriter BulkWriter = new(Dialect);
}