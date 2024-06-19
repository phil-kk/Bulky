using System;
using System.Collections.Concurrent;
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
    private static ConcurrentDictionary<Type, object> enumCache = new ();
    private static (object Value, NpgsqlDbType? Type) GetValue(object value)
    {
        if (value is not null)
        {
            var type = value.GetType() == typeof(Nullable<>) ? Nullable.GetUnderlyingType(value.GetType()) : value.GetType();
            if (type.IsEnum)
            {
                if (enumCache.TryGetValue(type, out object v))
                {
                    return (Value: v, Type: null);
                }
                var underlying = Enum.GetUnderlyingType(type);
                var enumValue = Convert.ChangeType(value, underlying);
                enumCache.TryAdd(type, enumValue);
                return (Value: enumValue, Type: null);
            }

            if (type == typeof(DateTime))
            {
                return (Value: value, Type: NpgsqlDbType.Date);
            }
            if (type == typeof(decimal))
            {
                return (Value: value, Type: NpgsqlDbType.Numeric);
            }
            if (type == typeof(double))
            {
                return (Value: value, NpgsqlDbType.Double);
            }
            return (Value: value, Type: null);
        }
        return (Value: null, Type:null);
    }

    public void Write<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items,
        IEnumerable<KeyValuePair<string, Member>> mapping, string tableName)
    {
        var columnsMapping = new List<KeyValuePair<string, Member>>(mapping);
        var columns = columnsMapping.Select(x => x.Key);
        var columnsString = string.Join(",", columns.Select(x => $"\"{x}\""));
        var accessor = TypeAccessor.Create(typeof(T));
        using var writer = (connection as NpgsqlConnection)?.BeginBinaryImport($"COPY \"{tableName}\" ({columnsString}) FROM STDIN (FORMAT BINARY)");
        writer.Timeout = TimeSpan.FromDays(1);
        var row = new object[columnsMapping.Count];
        foreach (var item in items)
        { 
            writer.StartRow();
            foreach (var value in columnsMapping.Select(columnMapping => accessor[item, columnMapping.Value.Name]))
            {
                var resultValue = GetValue(value);
                if (resultValue.Value is null)
                {
                    writer.WriteNull();
                }
                else
                {
                    if (resultValue.Type is null)
                    {
                        writer.Write(resultValue.Value);
                    }
                    else
                    {
                        writer.Write(resultValue.Value, resultValue.Type.Value);
                    }
                }
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
        writer.Timeout = TimeSpan.FromDays(1);
        var row = new object[columnsMapping.Count];
        foreach (var item in items)
        {
            await writer.StartRowAsync();
            foreach (var value in columnsMapping.Select(columnMapping => accessor[item, columnMapping.Value.Name]))
            {
                var resultValue = GetValue(value);
                if (resultValue.Value is null)
                {
                    await writer.WriteNullAsync();
                }
                else
                {
                    if (resultValue.Type is null)
                    {
                        await writer.WriteAsync(resultValue.Value);
                    }
                    else
                    {
                        await writer.WriteAsync(resultValue.Value, resultValue.Type.Value);
                    }
                }
            }
        }

        await writer.CompleteAsync();
    }
}