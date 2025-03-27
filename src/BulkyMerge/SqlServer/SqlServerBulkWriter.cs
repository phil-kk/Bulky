using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using BulkyMerge.Root;

namespace BulkyMerge.SqlServer;


internal class SqlServerBulkWriter : IBulkWriter
{
    private readonly ISqlDialect _dialect;

    public SqlServerBulkWriter(ISqlDialect dialect)
    {
        _dialect = dialect;
    }

    public void Write<T>(string destination, MergeContext<T> context)
    {
        var objectReader = context.Items.ToObjectDapperReader(_dialect, context.ColumnsToProperty.Select(x => x.Value.Name).ToArray());
        using var microsoftClientBukCopy = new SqlBulkCopy(context.Connection as SqlConnection, SqlBulkCopyOptions.Default, context.Transaction as SqlTransaction);
        foreach (var columnMapping in context.ColumnsToProperty)
        {
            microsoftClientBukCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
        }
        microsoftClientBukCopy.EnableStreaming = true;
        microsoftClientBukCopy.BatchSize = context.BatchSize;
        microsoftClientBukCopy.BulkCopyTimeout = context.Timeout;
        microsoftClientBukCopy.DestinationTableName = destination;

        microsoftClientBukCopy.WriteToServer(objectReader);
    }

    public async Task WriteAsync<T>(string destination, MergeContext<T> context)
    {
        var objectReader = context.Items.ToObjectDapperReader(_dialect, context.ColumnsToProperty.Select(x => x.Value.Name).ToArray());
        using var microsoftClientBukCopy = new SqlBulkCopy(context.Connection as SqlConnection, SqlBulkCopyOptions.Default, context.Transaction as SqlTransaction);
        foreach (var columnMapping in context.ColumnsToProperty)
        {
            microsoftClientBukCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
        }
        microsoftClientBukCopy.EnableStreaming = true;
        microsoftClientBukCopy.BatchSize = context.BatchSize;
        microsoftClientBukCopy.BulkCopyTimeout = context.Timeout;
        microsoftClientBukCopy.DestinationTableName = destination;

        await microsoftClientBukCopy.WriteToServerAsync(objectReader);
    }
}