using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using FastMember;
using System;

namespace BulkyMerge.Root;

internal static partial class BulkExtensions
{
    private static async Task<MergeContext<T>> BuildContextAsync<T>(ISqlDialect sqlDialect,
            DbConnection connection,
            IEnumerable<T> items,
            string tableName,
            DbTransaction transaction,
            int batchSize,
            IEnumerable<string> excludeProperties,
            IEnumerable<string> primaryKeys,
            int timeout)
    {
        var type = typeof(T);
        var typeAccessor = TypeAccessor.Create(type);
        var tableAttribute = type.GetCustomAttribute<TableAttribute>(true);
        var memberSet = typeAccessor.GetMembers().Where(x => x.GetAttribute(typeof(NotMappedAttribute), true) is null).ToList();
        primaryKeys ??= memberSet.Where(x => x.GetAttribute(typeof(KeyAttribute), true) is not null).Select(x =>
            x.GetAttribute(typeof(ColumnAttribute), true) is ColumnAttribute column ? column?.Name : x.Name).ToList();
        if (!primaryKeys.Any() && sqlDialect != null)
        {
            primaryKeys = await FindPrimaryKeysInfoAsync(sqlDialect, connection, transaction, tableName);
        }
        var identity = sqlDialect != null ? await FindIdentityInfoAsync(sqlDialect, connection, transaction, tableName) : null;
        var columnsToProperties = memberSet.ToDictionary(x =>
            x.GetAttribute(typeof(ColumnAttribute), true) is ColumnAttribute column ? column?.Name : x.Name);
        var tempTableName = sqlDialect?.GetTempTableName(tableName);
        return new MergeContext<T>(connection,
            transaction,
            items,
            typeAccessor,
            tableName ?? tableAttribute?.Name ?? type.Name,
            tableAttribute?.Schema ?? sqlDialect.DefaultScheme,
            tempTableName,
            columnsToProperties.Where(x => excludeProperties?.Any(c => c == x.Key) != true).ToDictionary(),
            identity,
            primaryKeys.ToList(),
            batchSize,
            timeout);
    }

    internal static async Task BulkCopyAsync<T>(IBulkWriter bulkWriter, DbConnection connection,
        DbTransaction transaction,
        IEnumerable<T> items,
        string tableName = default,
        IEnumerable<string> excludeColumns = default,
        int timeout = int.MaxValue,
        int batchSize = DefaultBatchSize)
    {
        var shouldCloseConnection =  await OpenConnectionAsync(connection);

        var context = await BuildContextAsync(null, connection, items, tableName, transaction, batchSize, null, null, timeout);
        await bulkWriter.WriteAsync(context.TableName, context);
        if (shouldCloseConnection) await connection.CloseAsync();
    }

     internal static Task BulkInsertOrUpdateAsync<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
            IEnumerable<T> items,
            string tableName = default,
            DbTransaction transaction = default,
            int batchSize = DefaultBatchSize,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue)
    => ExecuteInternalAsync(
            (dialect, context) => dialect.GetInsertOrUpdateMergeStatement(context.ColumnsToProperty.Keys, context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout, true);

    internal static async Task ExecuteInternalAsync<T>(
            Func<ISqlDialect, MergeContext<T>, string> dialectCall,
            IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
            IEnumerable<T> items,
            string tableName = default,
            DbTransaction transaction = default,
            int batchSize = DefaultBatchSize,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue,
            bool mapIdentity = false)
    {
        var shouldCloseConnection = await OpenConnectionAsync(connection);

        var context = await BuildContextAsync(dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout);
        await WriteToTempAsync(bulkWriter,
            dialect,
            context);

        var merge = dialectCall(dialect, context);

        if (!mapIdentity || context.Identity is null)
        {
            await ExecuteAsync(connection, merge, transaction);
            if (shouldCloseConnection) await connection.CloseAsync();
            return;
        }

        await using (var reader = await ExecuteReaderAsync(connection, merge, transaction))
        {
            MapIdentity(reader, context);
        }
        if (shouldCloseConnection) await connection.CloseAsync();
    }
     
     private static async Task WriteToTempAsync<T>(IBulkWriter bulkWriter, 
         ISqlDialect dialect,
         MergeContext<T> context,
         bool excludePrimaryKeys = false)
     {
         await CreateTemporaryTableAsync(dialect, context.Connection, context.Transaction, context.Identity, context.TableName, context.TempTableName, context.ColumnsToProperty.Select(x => x.Key));
         await bulkWriter.WriteAsync(context.TempTableName, context);
     }
    internal static Task BulkInsertAsync<T>(IBulkWriter bulkWriter, ISqlDialect dialect, 
         DbConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => ExecuteInternalAsync(
            (dialect, context) => dialect.GetInsertQuery(context.ColumnsToProperty.Keys, context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout, true);

    internal static Task BulkUpdateAsync<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => ExecuteInternalAsync(
            (dialect, context) => dialect.GetUpdateQuery(context.ColumnsToProperty.Keys, context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout);

    internal static Task BulkDeleteAsync<T>(IBulkWriter bulkWriter, 
         ISqlDialect dialect, 
         DbConnection connection,
         IEnumerable<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => ExecuteInternalAsync(
            (dialect, context) => dialect.GetDeleteQuery(context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, null, primaryKeys, timeout);

    private static Task CreateTemporaryTableAsync(ISqlDialect dialect, DbConnection connection, 
        DbTransaction transaction, 
        Identity identity, 
        string tableName, 
        string tempTableName, 
        IEnumerable<string> columnNames = default)
    {
        var queryString = new StringBuilder(dialect.GetCreateTempTableQuery(tempTableName, tableName, columnNames));
        if (identity is not null)
        {
            queryString.AppendLine(dialect.GetAlterIdentityColumnQuery(tempTableName, identity));
        }

        return ExecuteAsync(connection, queryString.ToString(), transaction);
    }

    private static async Task<Identity> FindIdentityInfoAsync(ISqlDialect dialect, DbConnection connection, DbTransaction transaction, string tableName)
    {
        await using var reader = await ExecuteReaderAsync(connection, dialect.GetFindIdentityQuery(connection.Database, tableName), transaction);
        return await reader.ReadAsync() ? new Identity(reader[0].ToString(), reader[1].ToString()) : null;
    }
    
    private static async Task<IEnumerable<string>> FindPrimaryKeysInfoAsync(ISqlDialect dialect, DbConnection connection, DbTransaction transaction, string tableName)
    {
        var result = new List<string>();
        await using var reader = await ExecuteReaderAsync(connection, dialect.GetFindPrimaryKeysQuery(connection.Database, tableName), transaction);
        while (await reader.ReadAsync())
        {
            result.Add(reader[0].ToString());
        }

        return result;
    }
    
    private static async Task<bool> OpenConnectionAsync(DbConnection connection)
    {
        if (connection.State is ConnectionState.Open) return false;
        await connection.OpenAsync();
        return true;
    }
    
    private static async Task ExecuteAsync(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        await command.ExecuteNonQueryAsync();
    }
    
    private static async Task<DbDataReader> ExecuteReaderAsync(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return await command.ExecuteReaderAsync();
    }
}