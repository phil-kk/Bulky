using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using FastMember;

namespace Bulky.Root;

public static partial class BulkExtensions
{
    public const int MemberSetCacheLimit = 10000;
    public const int DefaultBatchSize = 1000;
    private record struct CacheItem(
        TypeAccessor TypeAccessor, string TableName, string Schema, Dictionary<string, Member> ColumnsToProperty, List<string> PrimaryKeys)
    {
        public IEnumerable<string> ColumnNames => ColumnsToProperty.Keys;

        public volatile int HitPoints = 0;
    }

    private static volatile int _collected;
    private static readonly ConcurrentDictionary<Type, CacheItem> MemberSetCache = new ();

    private static CacheItem GetTypeCacheItem<T>()
    {
        if (MemberSetCache.TryGetValue(typeof(T), out var cacheItem))
        {
            
            Interlocked.Increment(ref cacheItem.HitPoints);
            return cacheItem;
        }
        if (Interlocked.Increment(ref _collected) >= MemberSetCacheLimit)
        {
            foreach (var item in MemberSetCache)
            {
                var temp = item.Value.HitPoints;
                if (Interlocked.CompareExchange(ref temp, 0, 0) <= 0)
                {
                    MemberSetCache.TryRemove(item);
                }
            }
        }

        var typeAccessor = TypeAccessor.Create(typeof(T));
        var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>(true);
        var memberSet = typeAccessor.GetMembers().Where(x => x.GetAttribute(typeof(NotMappedAttribute), true) is null).ToList();
        var primaryKeys = memberSet.Where(x => x.GetAttribute(typeof(KeyAttribute), true) is not null).Select(x => 
            x.GetAttribute(typeof(ColumnAttribute), true) is ColumnAttribute column ? column?.Name : x.Name).ToList();
        var columnsToProperties = memberSet.ToDictionary(x =>
            x.GetAttribute(typeof(ColumnAttribute), true) is ColumnAttribute column ? column?.Name : x.Name);
        cacheItem = new CacheItem
        {
            Schema = tableAttribute?.Schema ?? "public", 
            TableName = tableAttribute?.Name ?? typeof(T).Name, 
            TypeAccessor = typeAccessor, 
            ColumnsToProperty = columnsToProperties,
            PrimaryKeys = primaryKeys.Count is 0 ? null : primaryKeys
        };
        MemberSetCache.TryAdd(typeof(T), cacheItem);
        return cacheItem;
    }

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
    private static void MapIdentity<T>(IEnumerable<T> items, IDataReader reader, Identity identity)
    {
        if (identity is null) return;
        var cacheItem = GetTypeCacheItem<T>();
        var identityTypeCacheItem = cacheItem.ColumnsToProperty.Where(x =>
            x.Key.Equals(identity.ColumnName, StringComparison.InvariantCultureIgnoreCase)).Select(x => x.Value).First();
        var identityType = identityTypeCacheItem.Type;
        var defaultIdentityValue = !identityType.IsGenericType ? Activator.CreateInstance(identityType) : null;
        var identityName = identityTypeCacheItem.Name;
        foreach (var item in items)
        {
            var value = cacheItem.TypeAccessor[item, identityName];
            if (!object.Equals(defaultIdentityValue, value)) 
                continue;
            if (reader.Read())
            {
                cacheItem.TypeAccessor[item, identityName] = Convert(reader[0]);
            }
        }
    }
}