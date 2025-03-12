using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using FastMember;
using Microsoft.Data.SqlClient;
using Bulky.Root;

namespace Bulky.SqlServer;


public class SqlServerBulkWriter : IBulkWriter
{
    private readonly ISqlDialect _dialect;

    public SqlServerBulkWriter(ISqlDialect dialect)
    {
        _dialect = dialect;
    }

    public void Write<T>(DbConnection connection, 
        DbTransaction transaction, 
        int timeout, 
        int batchSize, 
        IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, 
        string tableName)
    {
        var objectReader = items.ToObjectDapperReader(_dialect, mapping.Select(x => x.Value.Name).ToArray());
        using var microsoftClientBukCopy = new SqlBulkCopy(connection as SqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction);
        foreach (var columnMapping in mapping)
        {
            microsoftClientBukCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
        }
        microsoftClientBukCopy.EnableStreaming = true;
        microsoftClientBukCopy.BatchSize = batchSize;
        microsoftClientBukCopy.BulkCopyTimeout = timeout;
        microsoftClientBukCopy.DestinationTableName = tableName;

        microsoftClientBukCopy.WriteToServer(objectReader);
    }

    public async Task WriteAsync<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        var objectReader = items.ToObjectDapperReader(_dialect, mapping.Select(x => x.Value.Name).ToArray());
        using var microsoftClientBukCopy = new SqlBulkCopy(connection as SqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction);
        foreach (var columnMapping in mapping)
        {
            microsoftClientBukCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
        }
        microsoftClientBukCopy.EnableStreaming = true;
        microsoftClientBukCopy.BatchSize = batchSize;
        microsoftClientBukCopy.BulkCopyTimeout = timeout;
        microsoftClientBukCopy.DestinationTableName = tableName;

        await microsoftClientBukCopy.WriteToServerAsync(objectReader);
    }
}