using System.Data.Common;
using FastBulkOperations.Root;
using FastMember;
using MySqlConnector;

namespace Dapper.FastBulkOperations.MySql;

public sealed class MySqlBulkWriter : IBulkWriter
{
    private readonly ISqlDialect _dialect;

    public MySqlBulkWriter(ISqlDialect dialect)
    {
        _dialect = dialect;
    }
    public void Write<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        if (connection is not MySqlConnection mySqlConnection) return;
        var ordered = mapping.OrderBy(x => x.Key).ToArray();
        var objectReader = items.ToObjectDapperReader(_dialect, ordered.Select(x => x.Value.Name).ToArray());
        var bulkCopy = new MySqlBulkCopy(mySqlConnection, transaction as MySqlTransaction);
        var ordinal = 0;
        foreach (var columnMapping in ordered)
        {
            bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping { DestinationColumn = columnMapping.Key, SourceOrdinal = ordinal++ } );
        }
        bulkCopy.BulkCopyTimeout = timeout;
        bulkCopy.DestinationTableName = tableName;
                    
        bulkCopy.WriteToServer(objectReader);
    }

    public async Task WriteAsync<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        if (connection is not MySqlConnection mySqlConnection) return;
        var ordered = mapping.OrderBy(x => x.Key).ToArray();
        var objectReader = items.ToObjectDapperReader(_dialect, ordered.Select(x => x.Value.Name).ToArray());
        var bulkCopy = new MySqlBulkCopy(mySqlConnection, transaction as MySqlTransaction);
        var ordinal = 0;
        foreach (var columnMapping in ordered)
        {
            bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping { DestinationColumn = columnMapping.Key, SourceOrdinal = ordinal++ } );
        }
        bulkCopy.BulkCopyTimeout = timeout;
        bulkCopy.DestinationTableName = tableName;
                    
        await bulkCopy.WriteToServerAsync(objectReader);
    }
}