using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using FastBulkOperations.Root;
using FastMember;
using Microsoft.Data.SqlClient;
using SystemSqlBulkCopyColumnMapping = System.Data.SqlClient.SqlBulkCopyColumnMapping;
using SystemSqlBulkCopyOptions = System.Data.SqlClient.SqlBulkCopyOptions;
using SystemSqlConnection = System.Data.SqlClient.SqlConnection;
using SystemSqlTransaction = System.Data.SqlClient.SqlTransaction;
using SystemSqlBulkCopy = System.Data.SqlClient.SqlBulkCopy;

namespace Dapper.FastBulkOperations.SqlServer;

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
        switch (connection)
        {
            case SystemSqlConnection systemSqlConnection:
            {
                using var systemClientBulCopy = new SystemSqlBulkCopy(systemSqlConnection,
                    SystemSqlBulkCopyOptions.Default, transaction as SystemSqlTransaction);
                foreach (var columnMapping in mapping)
                {
                    systemClientBulCopy.ColumnMappings.Add(new SystemSqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
                }
                systemClientBulCopy.EnableStreaming = true;
                systemClientBulCopy.BatchSize = batchSize;
                systemClientBulCopy.BulkCopyTimeout = timeout;
                systemClientBulCopy.DestinationTableName = tableName;
                    
                systemClientBulCopy.WriteToServer(objectReader);

                break;
            }
            case SqlConnection sqlConnection:
            {
                using var microsoftClientBukCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction);
                foreach (var columnMapping in mapping)
                {
                    microsoftClientBukCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
                }
                microsoftClientBukCopy.EnableStreaming = true;
                microsoftClientBukCopy.BatchSize = batchSize;
                microsoftClientBukCopy.BulkCopyTimeout = timeout;
                microsoftClientBukCopy.DestinationTableName = tableName;
                
                microsoftClientBukCopy.WriteToServer(objectReader);
                break;
            }
        }
    }

    public async Task WriteAsync<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        var objectReader = items.ToObjectDapperReader(_dialect, mapping.Select(x => x.Value.Name).ToArray());
        switch (connection)
        {
            case SystemSqlConnection systemSqlConnection:
            {
                using var systemClientBulCopy = new SystemSqlBulkCopy(systemSqlConnection,
                    SystemSqlBulkCopyOptions.Default, transaction as SystemSqlTransaction);
                foreach (var columnMapping in mapping)
                {
                    systemClientBulCopy.ColumnMappings.Add(new SystemSqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
                }
                systemClientBulCopy.EnableStreaming = true;
                systemClientBulCopy.BatchSize = batchSize;
                systemClientBulCopy.BulkCopyTimeout = timeout;
                systemClientBulCopy.DestinationTableName = tableName;
                    
                await systemClientBulCopy.WriteToServerAsync(objectReader);

                break;
            }
            case SqlConnection sqlConnection:
            {
                using var microsoftClientBukCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction);
                foreach (var columnMapping in mapping)
                {
                    microsoftClientBukCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(columnMapping.Value.Name, columnMapping.Key));
                }
                microsoftClientBukCopy.EnableStreaming = true;
                microsoftClientBukCopy.BatchSize = batchSize;
                microsoftClientBukCopy.BulkCopyTimeout = timeout;
                microsoftClientBukCopy.DestinationTableName = tableName;
                
                await microsoftClientBukCopy.WriteToServerAsync(objectReader);
                break;
            }
        }
    }
}