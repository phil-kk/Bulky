using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

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
}