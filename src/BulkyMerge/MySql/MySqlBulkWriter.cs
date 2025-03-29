using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using BulkyMerge.Root;
using FastMember;
using MySqlConnector;

namespace BulkyMerge.MySql;

internal sealed class MySqlBulkWriter : IBulkWriter
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
    public void Write<T>(string destination, MergeContext<T> context)
       => WriteAsync(destination, context).GetAwaiter().GetResult();

    public async Task WriteAsync<T>(string destination, MergeContext<T> context)
    {
        if (context.Connection is not MySqlConnection mySqlConnection) return;
        await SetLoadInFileAsync(mySqlConnection);
        var ordered = context.ColumnsToProperty.OrderBy(x => x.Key).ToArray();
        var objectReader = context.Items.ToObjectDapperReader(_dialect, ordered.Select(x => x.Value.Name).ToArray());
        var bulkCopy = new MySqlBulkCopy(mySqlConnection, context.Transaction as MySqlTransaction);
        var ordinal = 0;
        foreach (var columnMapping in ordered)
        {
            bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping { DestinationColumn = columnMapping.Key, SourceOrdinal = ordinal++ } );
        }
        bulkCopy.BulkCopyTimeout = context.Timeout;
        bulkCopy.DestinationTableName = destination;
                    
        await bulkCopy.WriteToServerAsync(objectReader);
    }
}