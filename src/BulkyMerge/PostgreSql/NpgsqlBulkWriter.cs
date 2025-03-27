using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using BulkyMerge.Root;
using FastMember;
using Npgsql;
using NpgsqlTypes;

namespace BulkyMerge.PostgreSql.PostgreSql;

internal sealed class NpgsqlBulkWriter : IBulkWriter
{
    private static readonly ConcurrentDictionary<Type, object> enumCache = new();

    private Dictionary<string, string> GetColumns(DbConnection connection, string tableName)
    {
        var columns = new Dictionary<string, string>();
        try
        {
            var query = @"SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = @tableName";

            using (var cmd = new NpgsqlCommand(query, connection as NpgsqlConnection))
            {
                cmd.Parameters.AddWithValue("tableName", tableName);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns[reader.GetString(0).ToLower()]= reader.GetString(1);
                    }
                }
            }
        }
        catch (Exception e)
        {
            ;
        }
        return columns;
    }
        
    private object PrepareValue(object value, string type)
    {
        if (value != null)
        {
            if (type == "timestamp without time zone"
                && (value.GetType() == typeof(DateTime) || value.GetType() == typeof(DateTime?)))
            {
                try
                {
                    return DateTime.SpecifyKind((DateTime)value, DateTimeKind.Unspecified);
                }
                catch
                {
                    return value;
                }
            }
        }
        return value;
    }


    public void Write<T>(string destination, MergeContext<T> context)
    {
        var columnTypes = GetColumns(context.Connection, context.TableName);
        var columnsMapping = new List<KeyValuePair<string, Member>>(context.ColumnsToProperty);
        var columns = columnsMapping.Select(x => x.Key);
        var columnsString = string.Join(",", columns.Select(x => $"\"{x}\""));
        var accessor = TypeAccessor.Create(typeof(T));
        using var writer = (context.Connection as NpgsqlConnection)?.BeginBinaryImport($"COPY \"{destination}\" ({columnsString}) FROM STDIN (FORMAT BINARY)");
        writer.Timeout = TimeSpan.FromDays(1);
        var row = new object[columnsMapping.Count];
        foreach (var item in context.Items)
        {
            writer.StartRow();
            foreach (var columnMapping in columnsMapping)
            {
                var commonConverter = TypeConverters.GetConverter(columnMapping.Value.Type);
                var value = accessor[item, columnMapping.Value.Name];
                var type = columnTypes.TryGetValue(columnMapping.Key.ToLower(), out var columnType) ? columnType : null;
                if (commonConverter != null)
                {
                    value = commonConverter.Convert(value);
                }
                if (type != null)
                {
                    if (value is null)
                    {
                        writer.WriteNull();
                    }
                    else
                    {
                        writer.Write(PrepareValue(value, type), type);
                    }
                }
            }
        }

        writer.Complete();
    }

    public async Task WriteAsync<T>(string destination, MergeContext<T> context)
    {
        var columnTypes = GetColumns(context.Connection, context.TableName);
        var columnsMapping = new List<KeyValuePair<string, Member>>(context.ColumnsToProperty);
        var columns = columnsMapping.Select(x => x.Key);
        var columnsString = string.Join(",", columns.Select(x => $"\"{x}\""));
        var accessor = TypeAccessor.Create(typeof(T));
        await using var writer = await (context.Connection as NpgsqlConnection)?.BeginBinaryImportAsync($"COPY \"{destination}\" ({columnsString}) FROM STDIN (FORMAT BINARY)");
        writer.Timeout = TimeSpan.FromDays(1);
        var row = new object[columnsMapping.Count];
        foreach (var item in context.Items)
        {
            var vals = new List<(object Value, NpgsqlDbType? Type)>();
            await writer.StartRowAsync();
            foreach (var columnMapping in columnsMapping)
            {
                var commonConverter = TypeConverters.GetConverter(columnMapping.Value.Type);
                var value = accessor[item, columnMapping.Value.Name];
                var type = columnTypes.TryGetValue(columnMapping.Key.ToLower(), out var columnType) ? columnType : null;
                if (commonConverter != null)
                {
                    value = commonConverter.Convert(value);
                }
                if (type != null)
                {
                    if (value is null)
                    {
                        await writer.WriteNullAsync();
                    }
                    else
                    {
                        await writer.WriteAsync(PrepareValue(value, type), type);
                    }
                }
            }
            var log = string.Join(", ", vals.Select(x => $"({x.Value}, {x.Type})"));
            ;

        }

        await writer.CompleteAsync();
    }
}