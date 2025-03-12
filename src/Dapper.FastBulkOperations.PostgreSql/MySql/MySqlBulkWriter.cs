using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Bulky.Root;
using FastMember;
using MySqlConnector;

namespace Bulky.MySql;

public sealed class MySqlBulkWriter : IBulkWriter
{
    private readonly ISqlDialect _dialect;

    public MySqlBulkWriter(ISqlDialect dialect)
    {
        _dialect = dialect;
    }

    private void SetLoadInFile(MySqlConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SET GLOBAL local_infile = true;";
        cmd.ExecuteNonQuery();
    }
    
    private async Task SetLoadInFileAsync(MySqlConnection connection)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SET GLOBAL local_infile = true;";
        await cmd.ExecuteNonQueryAsync();
    }
    public void Write<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        if (connection is not MySqlConnection mySqlConnection) return;
        SetLoadInFile(mySqlConnection);
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
        await SetLoadInFileAsync(mySqlConnection);
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