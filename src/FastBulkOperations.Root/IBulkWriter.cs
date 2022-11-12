using System.Data.Common;
using FastMember;

namespace FastBulkOperations.Root;

public interface IBulkWriter
{
    void Write<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items, IEnumerable<KeyValuePair<string, Member>> mapping, string tableName);
    Task WriteAsync<T>(DbConnection connection, DbTransaction transaction, int timeout, int batchSize, IEnumerable<T> items, IEnumerable<KeyValuePair<string, Member>> mapping, string tableName);
}