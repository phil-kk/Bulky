using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using FastBulkOperations.Root;
using FastMember;
using Npgsql;
using NpgsqlTypes;

namespace Dapper.FastBulkOperations.PostgreSql;

public sealed class NpgsqlBulkWriter : IBulkWriter
{
    public void Write<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        var columnsMapping = new List<KeyValuePair<string, Member>>(mapping);
        var columns = columnsMapping.Select(x => x.Key);
        var columnsString = string.Join(",", columns.Select(x => $"\"{x}\""));
        var accessor = TypeAccessor.Create(typeof(T));
        using var writer = (connection as NpgsqlConnection)?.BeginBinaryImport($"COPY \"{tableName}\" ({columnsString}) FROM STDIN (FORMAT BINARY)");
        var row = new object[columnsMapping.Count];
        foreach (var item in items)
        { 
            writer.StartRow();
            foreach (var value in columnsMapping.Select(columnMapping => accessor[item, columnMapping.Value.Name]))
            {
                if (value is not null)
                {
                    var type = value.GetType() == typeof(Nullable<>) ? Nullable.GetUnderlyingType(value.GetType()) : value.GetType();
                    if (type.IsEnum)
                    {
                        var underlying = Enum.GetUnderlyingType(type);
                        writer.Write(Convert.ChangeType(value, underlying));
                        continue;
                    }

                    if (type == typeof(DateTime))
                    {
                        writer.Write(value, NpgsqlDbType.Date);
                        continue;
                    }

                    if (type == typeof(decimal) || type == typeof(double))
                    {
                        writer.Write(Convert.ChangeType(value, typeof(double)), NpgsqlDbType.Double);
                        continue;
                    }
                    writer.Write(value);
                    continue;
                }
                writer.WriteNull();
            }
        }

        writer.Complete();
    }

    public async Task WriteAsync<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        var columnsMapping = new List<KeyValuePair<string, Member>>(mapping);
        var columns = columnsMapping.Select(x => x.Key);
        var columnsString = string.Join(",", columns.Select(x => $"\"{x}\""));
        var accessor = TypeAccessor.Create(typeof(T));
        await using var writer = await (connection as NpgsqlConnection)?.BeginBinaryImportAsync($"COPY \"{tableName}\" ({columnsString}) FROM STDIN (FORMAT BINARY)");
        var row = new object[columnsMapping.Count];
        foreach (var item in items)
        {
            await writer.StartRowAsync();
            foreach (var value in columnsMapping.Select(columnMapping => accessor[item, columnMapping.Value.Name]))
            {
                if (value is not null)
                {
                    var type = value.GetType() == typeof(Nullable<>) ? Nullable.GetUnderlyingType(value.GetType()) : value.GetType();
                    if (type.IsEnum)
                    {
                        var underlying = Enum.GetUnderlyingType(type);
                        await writer.WriteAsync(Convert.ChangeType(value, underlying));
                        continue;
                    }

                    if (type == typeof(DateTime))
                    {
                        await writer.WriteAsync(value, NpgsqlDbType.Date);
                        continue;
                    }

                    if (type == typeof(decimal) || type == typeof(double))
                    {
                        await writer.WriteAsync(Convert.ChangeType(value, typeof(double)), NpgsqlDbType.Double);
                        continue;
                    }
                    await writer.WriteAsync(value);
                    continue;
                }
                await writer.WriteNullAsync();
            }
        }

        await writer.CompleteAsync();
    }
}