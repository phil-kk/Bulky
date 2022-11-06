using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using FastMember;
using Microsoft.Data.SqlClient;
using SystemSqlBulkCopyColumnMapping = System.Data.SqlClient.SqlBulkCopyColumnMapping;
using SystemSqlBulkCopyOptions = System.Data.SqlClient.SqlBulkCopyOptions;
using SystemSqlConnection = System.Data.SqlClient.SqlConnection;
using SystemSqlTransaction = System.Data.SqlClient.SqlTransaction;
using SystemSqlBulkCopy = System.Data.SqlClient.SqlBulkCopy;

namespace Dapper.FastBulkOperations.SqlServer;

internal sealed class BulkWriter<T> : IAsyncDisposable, IDisposable
{
    private readonly SqlBulkCopy _microsoftClientBukCopy;
    private readonly SystemSqlBulkCopy _systemClientBulCopy;
    private readonly DapperObjectReader<T> _objectReader;
    public BulkWriter(IEnumerable<T> items, 
        IDbConnection connection, 
        IDbTransaction transaction, 
        int batchSize, 
        int bulkCopyTimeout, 
        string target, 
        IEnumerable<KeyValuePair<string, Member>> columnsMapping,
        string[] propertyNames)
    {
        _objectReader = items.ToObjectDapperReader(propertyNames);
        switch (connection)
        {
            case SystemSqlConnection systemSqlConnection:
            {
                _systemClientBulCopy = new SystemSqlBulkCopy(systemSqlConnection, SystemSqlBulkCopyOptions.Default, transaction as SystemSqlTransaction);
                foreach (var mapping in columnsMapping)
                {
                    _systemClientBulCopy.ColumnMappings.Add(new SystemSqlBulkCopyColumnMapping(mapping.Value.Name, mapping.Key));
                }
                _systemClientBulCopy.EnableStreaming = true;
                _systemClientBulCopy.BatchSize = batchSize;
                _systemClientBulCopy.BulkCopyTimeout = bulkCopyTimeout;
                _systemClientBulCopy.DestinationTableName = target;
                break;
            }
            case SqlConnection sqlConnection:
            {
                _microsoftClientBukCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, transaction as SqlTransaction);
                foreach (var mapping in columnsMapping)
                {
                    _microsoftClientBukCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(mapping.Value.Name, mapping.Key));
                }
                _microsoftClientBukCopy.EnableStreaming = true;
                _microsoftClientBukCopy.BatchSize = batchSize;
                _microsoftClientBukCopy.BulkCopyTimeout = bulkCopyTimeout;
                _microsoftClientBukCopy.DestinationTableName = target;
                break;
            }
        }
    }

    public void Write()
    {
        if (_microsoftClientBukCopy != null)
        {
            try
            {
                _microsoftClientBukCopy.WriteToServer(_objectReader);
            }
            catch (Exception e)
            {
                throw new Exception("NOTE : Property names should have same Case as Target Table Name", e);
            }
        }
        else
        {
            try
            {
                _systemClientBulCopy?.WriteToServer(_objectReader);
            }
            catch (Exception e)
            {
                throw new Exception("NOTE : Property names should have same Case as Target Table Name", e);
            }
        }
    }

    public async Task WriteAsync()
    {
        if (_microsoftClientBukCopy != null)
        {
            try
            {
                await _microsoftClientBukCopy.WriteToServerAsync(_objectReader);
            }
            catch (Exception e)
            {
                throw new Exception("NOTE : Property names should have same Case as Target Table Name", e);
            }
        }
        else if (_systemClientBulCopy != null)
        {
            try
            {
                await _systemClientBulCopy.WriteToServerAsync(_objectReader);
            }
            catch (Exception e)
            {
                throw new Exception("NOTE : Property names should have same Case as Target Table Name", e);
            }
        }
        else
        {
            throw new Exception(
                "Connection type not suppoerted please use Microsoft.Data.SqlClient.SqlConnection or System.Data.SqlClient.SqlConnection");
        }
    }
    
    public ValueTask DisposeAsync()
    {
        _microsoftClientBukCopy?.Close();
        _systemClientBulCopy?.Close();
        return _objectReader.DisposeAsync();
    }

    public void Dispose()
    {
        _microsoftClientBukCopy?.Close();
        _systemClientBulCopy?.Close();
        _objectReader.Dispose();
    }
}