using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using FastMember;

namespace Bulky.Root;

public interface IBulkWriter
{
    void Write<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items, IEnumerable<KeyValuePair<string, Member>> mapping, string tableName);
    Task WriteAsync<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items, IEnumerable<KeyValuePair<string, Member>> mapping, string tableName);
}