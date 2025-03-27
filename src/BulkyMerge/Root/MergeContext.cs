using System.Collections.Generic;
using System.Data.Common;
using FastMember;

namespace BulkyMerge.Root;

internal record MergeContext<T>(
        DbConnection Connection,
        DbTransaction Transaction,
        IEnumerable<T> Items,
        TypeAccessor TypeAccessor,
        string TableName,
        string Schema,
        string TempTableName,
        Dictionary<string, Member> ColumnsToProperty,
        Identity Identity,
        List<string> PrimaryKeys,
        int BatchSize,
        int Timeout);
