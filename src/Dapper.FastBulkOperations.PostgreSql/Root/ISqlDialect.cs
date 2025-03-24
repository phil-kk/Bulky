﻿using System.Collections.Generic;
using System.Data;

namespace BulkyMerge.Root;

public record BulkWriteContext(
    string TableName,
    string TempTable,
    IEnumerable<string> PrimaryKeys,
    Identity Identity);


public record Identity(string ColumnName, string Type);

public interface ISqlDialect
{
    string GetFindPrimaryKeysQuery(string databaseName, string tableName);
    string GetFindIdentityQuery(string databaseName, string tableName);
    string GetCreateTempTableQuery(string tempTableName, string destination, IEnumerable<string> columnNames = null);
    string GetInsertOrUpdateMergeStatement(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext);
    string GetAlterIdentityColumnQuery(string tempTableName, Identity identity);
    string GetInsertQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext);
    string GetUpdateQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext);
    string GetDeleteQuery(BulkWriteContext createTempTableResult);
    string GetTempTableName(string targetTableName);
    IDbDataParameter CreateParameter(object value);
}