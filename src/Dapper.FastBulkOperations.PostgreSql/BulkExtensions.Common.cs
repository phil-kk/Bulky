namespace Dapper.FastBulkOperations.PostgreSql;

public static partial class NpgsqlBulkExtensions
{
    private static readonly NpgsqlDialect Dialect = new();
    private static readonly NpgsqlBulkWriter BulkWriter = new();
}