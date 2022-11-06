using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Dapper;
using FastMember;
using Microsoft.Data.SqlClient;

namespace Dapper.FastBulkOperations.SqlServer;

public static class ObjectReaderExtensions
{
    public static DapperObjectReader<T> ToObjectDapperReader<T>(this IEnumerable<T> enumerable, params string[] members)
    {
        return new DapperObjectReader<T>(enumerable, members);
    }
}

public sealed class DapperObjectReader<T> : ObjectReader
{
    private static readonly Dictionary<Type, SqlMapper.ITypeHandler> TypeHandler = typeof(SqlMapper).GetField("typeHandlers", BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Static)?.GetValue(null) as Dictionary<Type, SqlMapper.ITypeHandler>;

    public DapperObjectReader(IEnumerable<T> source, params string[] members) : base(typeof(T), source, members)
    {
    }

    private static object HandleValue(object value)
    {
        if (value is null || value == DBNull.Value) return null;
        var type = value.GetType();
        if (!TypeHandler.ContainsKey(type)) return value;
        var sqlParameter = new SqlParameter { Value = value };
        TypeHandler[type].SetValue(sqlParameter, value);
        return sqlParameter.Value;
    }
    
    private static readonly  object DbNull = (object) DBNull.Value;
    public override object this[string name] => HandleValue(base[name]) ?? DbNull;

    public override object this[int i] => HandleValue(base[i]) ?? DbNull;
}