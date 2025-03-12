using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using Dapper;
using FastMember;

namespace Bulky.Root;

public interface ITypeConverter
{
    object Convert(object value);
}

public static class TypeConverters
{
    private static readonly ConcurrentDictionary<Type, ITypeConverter> converters = new();
    public static void RegisterTypeConverter(Type type, ITypeConverter converter)
    {
        converters[type] = converter;
    }

    internal static ITypeConverter GetConverter(Type type) => converters.TryGetValue(type, out var converter) ? converter : null;
}

public static class ObjectReaderExtensions
{
    public static DapperObjectReader<T> ToObjectDapperReader<T>(this IEnumerable<T> enumerable, ISqlDialect sqlDialect, params string[] members)
    {
        return new DapperObjectReader<T>(sqlDialect, enumerable, members);
    }
}

public sealed class DapperObjectReader<T> : ObjectReader
{
    private static readonly Dictionary<Type, SqlMapper.ITypeHandler> _typeHandlers = typeof(SqlMapper).GetField("typeHandlers", BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Static)?.GetValue(null) as Dictionary<Type, SqlMapper.ITypeHandler>;
    private readonly ISqlDialect _sqlDialect;
    public DapperObjectReader(ISqlDialect sqlDialect, IEnumerable<T> source, params string[] members) : base(typeof(T), source, members)
    {
        _sqlDialect = sqlDialect;
    }

    private object HandleValue(object value)
    {
        if (value is null || value == DBNull.Value) return null;
        var type = value.GetType();
        var converter = TypeConverters.GetConverter(type);
        if (converter != null) return converter.Convert(value);
        if (!_typeHandlers.ContainsKey(type)) return value;
        var sqlParameter = _sqlDialect.CreateParameter(value);
        _typeHandlers[type].SetValue(sqlParameter, value);
        return sqlParameter.Value;
    }
    public override object this[string name] => HandleValue(base[name]) ?? DBNull.Value;

    public override object this[int i] => HandleValue(base[i]) ?? DBNull.Value;
}