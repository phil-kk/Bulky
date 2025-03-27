using System.Collections.Generic;
using System.Data;

namespace BulkyMerge.Root;

public record Identity(string ColumnName, string Type);

public interface ISqlDialect
{
    string DefaultScheme { get; }
    string GetFindPrimaryKeysQuery(string databaseName, string tableName);
    string GetFindIdentityQuery(string databaseName, string tableName);
    string GetCreateTempTableQuery(string tempTableName, string destination, IEnumerable<string> columnNames = null);
    string GetInsertOrUpdateMergeStatement(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity);
    string GetAlterIdentityColumnQuery(string tempTableName, Identity identity);
    string GetInsertQuery(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity);
    string GetUpdateQuery(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity);
    string GetDeleteQuery(string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity);
    string GetTempTableName(string targetTableName);
    IDbDataParameter CreateParameter(object value);
}