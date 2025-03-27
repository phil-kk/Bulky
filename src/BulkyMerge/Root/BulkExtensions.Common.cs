using System;
using System.Data;
using System.Linq;

namespace BulkyMerge.Root;

internal static partial class BulkExtensions
{
    public const int DefaultBatchSize = 1000;
    private static object Convert(object val)
    {
        if (val == null) return null;
        if (val is int i)
            val = i;
        if (val is uint ui)
            val = ui;
        if (val is short s)
            val = s;
        if (val is ushort us)
            val = us;
        if (val is long l)
            val = l;
        if (val is ulong ul)
            return ul;
        return val;
    }
    private static void MapIdentity<T>(IDataReader reader, MergeContext<T> context)
    {
        if (context.Identity is null) return;
        var identityTypeCacheItem = context.ColumnsToProperty.Where(x =>
            x.Key.Equals(context.Identity.ColumnName, StringComparison.InvariantCultureIgnoreCase)).Select(x => x.Value).First();
        var identityType = identityTypeCacheItem.Type;
        var defaultIdentityValue = !identityType.IsGenericType ? Activator.CreateInstance(identityType) : null;
        var identityName = identityTypeCacheItem.Name;
        foreach (var item in context.Items)
        {
            var value = context.TypeAccessor[item, identityName];
            if (!object.Equals(defaultIdentityValue, value)) 
                continue;
            if (reader.Read())
            {
                context.TypeAccessor[item, identityName] = Convert(reader[0]);
            }
        }
    }
}