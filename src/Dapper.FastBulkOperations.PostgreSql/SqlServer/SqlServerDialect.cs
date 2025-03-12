using Bulky.Root;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Bulky.SqlServer;

public sealed class SqlServerDialect : ISqlDialect
{
    public string GetFindPrimaryKeysQuery(string databaseName,string tableName)
        => $"SELECT COLUMN_NAME as ColumnName, DATA_TYPE as Type FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{tableName}'";


    public string GetFindIdentityQuery(string databaseName,string tableName)
        => $"SELECT COLUMN_NAME as ColumnName, DATA_TYPE as Type FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{tableName}'";


    public string GetCreateTempTableQuery(string tempTableName, string destination, IEnumerable<string> columnNames = null)=> $"SELECT {(columnNames is null ? "*" : string.Join(',', columnNames))} INTO {tempTableName} FROM {destination} WITH(READUNCOMMITTED) WHERE 1 = 0";

    public string GetInsertOrUpdateMergeStatement(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
    {
        var identityExist = bulkWriteContext.Identity is not null;
        columnNames = identityExist ? columnNames.Where(x => x != bulkWriteContext.Identity.ColumnName) : columnNames;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', columnNames.Select(x => $"[{x}]"));

        if (identityExist)
        {
            merge.Append($"DECLARE @Id TABLE ([Action] VARCHAR(20), [Id]");
            merge.Append(bulkWriteContext.Identity.Type);
            merge.Append(')');
        }
        merge.Append($@"
MERGE [{bulkWriteContext.TableName}] AS T  
USING [{bulkWriteContext.TempTable}] AS S
ON ({string.Join(" AND ", bulkWriteContext.PrimaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN NOT MATCHED
THEN INSERT ({columnsString}) VALUES ({columnsString})
WHEN MATCHED 
THEN UPDATE SET {string.Join(',', columnNames.Except(bulkWriteContext.PrimaryKeys).Select(x => $"T.[{x}] = S.[{x}]"))}");
        if (identityExist)
        {
            merge.AppendLine();
            merge.Append($"OUTPUT $action, inserted.");
            merge.Append(bulkWriteContext.Identity.ColumnName);
            merge.AppendLine(" INTO @Id ([Action], [Id]);");
            merge.AppendLine("SELECT [Id] FROM @Id WHERE [Action] = 'INSERT' ORDER BY [Id] ASC");
        }
        else
        {
            merge.AppendLine(";");
        }
        merge.AppendLine($"DROP TABLE {bulkWriteContext.TempTable}");
        return merge.ToString();
    }

    public string GetAlterIdentityColumnQuery(string tempTableName, Identity identity)
        => $@"ALTER TABLE {tempTableName} ADD _IfIdentityJustOneColumn_{identity.ColumnName} BIT
ALTER TABLE {tempTableName} DROP COLUMN {identity.ColumnName}
ALTER TABLE {tempTableName} ADD {identity.ColumnName} {identity.Type}
ALTER TABLE {tempTableName} DROP COLUMN _IfIdentityJustOneColumn_{identity.ColumnName}";

    public string GetInsertQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
    {
        var identityExist = bulkWriteContext.Identity is not null;
        var columnsString = string.Join(',', (!identityExist ? columnNames : columnNames.Where(x => x != bulkWriteContext.Identity.ColumnName)).Select(x => $"[{x}]"));
        var insert = new StringBuilder();
        if (identityExist)
        {
            insert.Append($"DECLARE @Id TABLE ([Id] {bulkWriteContext.Identity.Type})");
        }

        insert.Append($@"INSERT INTO [{bulkWriteContext.TableName}]({columnsString})");
        if (identityExist)
        {
            insert.AppendLine($"OUTPUT inserted.[{bulkWriteContext.Identity.ColumnName}] INTO @Id");
        }
        insert.AppendLine($"SELECT {columnsString} FROM {bulkWriteContext.TempTable}");
        if (identityExist)
        {
            insert.AppendLine("SELECT [Id] FROM @Id ORDER BY [Id] ASC");
        }
        insert.AppendLine($"DROP TABLE {bulkWriteContext.TempTable}");
        return insert.ToString();
    }

    public string GetUpdateQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext) => $@"
MERGE [{bulkWriteContext.TableName}] AS T  
USING (SELECT * FROM [{bulkWriteContext.TempTable}]) AS S
ON ({string.Join(" AND ", bulkWriteContext.PrimaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN MATCHED 
THEN UPDATE SET {string.Join(',', columnNames.Except(bulkWriteContext.PrimaryKeys).Select(x => $"T.[{x}] = S.[{x}]"))};
DROP TABLE {bulkWriteContext.TempTable}";

    public string GetDeleteQuery(BulkWriteContext createTempTableResult) => $@"
MERGE [{createTempTableResult.TableName}] AS T  
USING (SELECT * FROM [{createTempTableResult.TempTable}]) AS S
ON ({string.Join(" AND ", createTempTableResult.PrimaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN MATCHED
THEN DELETE;
DROP TABLE {createTempTableResult.TempTable}";

    public string GetTempTableName(string targetTableName) => $"#{targetTableName}";

    public IDbDataParameter CreateParameter(object value) => new SqlParameter { Value = value};

}