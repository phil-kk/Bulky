using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using FastMember;
using System;

namespace BulkyMerge.Root;

internal static partial class BulkExtensions
{
    private static MergeContext<T> BuildContext<T>(ISqlDialect sqlDialect,
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
            primaryKeys = FindPrimaryKeysInfo(sqlDialect, connection, transaction, tableName);
        }
        var identity = sqlDialect != null ? FindIdentityInfo(sqlDialect, connection, transaction, tableName) : null;
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

    private static bool OpenConnection(DbConnection connection)
    {
        if (connection.State is ConnectionState.Open) return false;
        connection.Open();
        return true;

    }
    
    internal static  void BulkCopy<T>(IBulkWriter bulkWriter, DbConnection connection,
        DbTransaction transaction,
        IEnumerable<T> items,
        string tableName = default,
        int timeout = int.MaxValue,
        IEnumerable<string> excludeColumns = default,
        int batchSize = DefaultBatchSize)
    {
        var shouldCloseConnection =  OpenConnection(connection);

        var context = BuildContext(null, connection, items, tableName, transaction, batchSize, null, null, timeout);
        bulkWriter.Write(context.TableName, context);
        if (shouldCloseConnection) connection.Close();
    }

    internal static void ExecuteInternal<T>(
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
        var shouldCloseConnection = OpenConnection(connection);

        var context = BuildContext(dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout);
        WriteToTemp(bulkWriter,
            dialect,
            context);

        var merge = dialectCall(dialect, context);

        if (!mapIdentity || context.Identity is null)
        {
            Execute(connection, merge, transaction);
            if (shouldCloseConnection) connection.Close();
            return;
        }

        using (var reader = ExecuteReader(connection, merge, transaction))
        {
            MapIdentity(reader, context);
        }
        if (shouldCloseConnection) connection.Close();
    }

    internal static  void BulkInsertOrUpdate<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
            IEnumerable<T> items,
            string tableName = default,
            DbTransaction transaction = default,
            int batchSize = DefaultBatchSize,
            IEnumerable<string> excludeProperties = default,
            IEnumerable<string> primaryKeys = default,
            int timeout = int.MaxValue)
    => ExecuteInternal(
            (dialect, context) => dialect.GetInsertOrUpdateMergeStatement(context.ColumnsToProperty.Keys, context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout, true);

    internal static void BulkInsert<T>(IBulkWriter bulkWriter, ISqlDialect dialect, 
         DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
    => ExecuteInternal(
            (dialect, context) => dialect.GetInsertQuery(context.ColumnsToProperty.Keys, context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout, true);

    internal static  void BulkUpdate<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         string[] excludeProperties = default,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => ExecuteInternal(
            (dialect, context) => dialect.GetUpdateQuery(context.ColumnsToProperty.Keys, context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, excludeProperties, primaryKeys, timeout, true);

    internal static void BulkDelete<T>(IBulkWriter bulkWriter, ISqlDialect dialect, DbConnection connection,
         IList<T> items,
         string tableName = default,
         DbTransaction transaction = default,
         int batchSize = DefaultBatchSize,
         IEnumerable<string> primaryKeys = default,
         int timeout = int.MaxValue)
     => ExecuteInternal(
            (dialect, context) => dialect.GetDeleteQuery(context.TableName, context.TempTableName, context.PrimaryKeys, context.Identity),
            bulkWriter, dialect, connection, items, tableName, transaction, batchSize, null, primaryKeys, timeout, true);

    private static void WriteToTemp<T>(IBulkWriter bulkWriter,
         ISqlDialect dialect,
         MergeContext<T> context,
         bool excludePrimaryKeys = false)
    {
        CreateTemporaryTable(dialect, context.Connection, context.Transaction, context.Identity, context.TableName, context.TempTableName, context.ColumnsToProperty.Select(x => x.Key));
        bulkWriter.Write(context.TempTableName, context);
    }

    private static void CreateTemporaryTable(ISqlDialect dialect, DbConnection connection,
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

        Execute(connection, queryString.ToString(), transaction);
    }

    private static Identity FindIdentityInfo(ISqlDialect dialect, DbConnection connection, DbTransaction transaction,
        string tableName)
    {
        using var reader = ExecuteReader(connection, dialect.GetFindIdentityQuery(connection.Database, tableName), transaction);
        return reader.Read() ? new Identity(reader[0].ToString(), reader[1].ToString()) : null;
    }

    private static IEnumerable<string> FindPrimaryKeysInfo(ISqlDialect dialect, DbConnection connection,
        DbTransaction transaction, string tableName)
    {
        using var reader = ExecuteReader(connection, dialect.GetFindPrimaryKeysQuery(connection.Database, tableName), transaction);
        while (reader.Read())
        {
            yield return reader[0].ToString();
        }
    }

    private static void Execute(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        command.ExecuteNonQuery();
    }

    private static DbDataReader ExecuteReader(DbConnection connection, string sql, DbTransaction transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;
        return command.ExecuteReader();
    }
}