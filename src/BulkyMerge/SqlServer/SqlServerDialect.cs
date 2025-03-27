using BulkyMerge.Root;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace BulkyMerge.SqlServer;

public sealed class SqlServerDialect : ISqlDialect
{
    public string DefaultScheme => "dbo";

    public string GetFindPrimaryKeysQuery(string databaseName,string tableName)
        => $"SELECT COLUMN_NAME as ColumnName, DATA_TYPE as Type FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{tableName}'";


    public string GetFindIdentityQuery(string databaseName,string tableName)
        => $"SELECT COLUMN_NAME as ColumnName, DATA_TYPE as Type FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1 AND TABLE_NAME = '{tableName}'";


    public string GetCreateTempTableQuery(string tempTableName, string destination, IEnumerable<string> columnNames = null)=> $"SELECT {(columnNames is null ? "*" : string.Join(',', columnNames))} INTO {tempTableName} FROM {destination} WITH(READUNCOMMITTED) WHERE 1 = 0";

    public string GetInsertOrUpdateMergeStatement(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity)
    {
        var identityExist = identity is not null;
        columnNames = identityExist ? columnNames.Where(x => x != identity.ColumnName) : columnNames;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', columnNames.Select(x => $"[{x}]"));

        if (identityExist)
        {
            merge.Append($"DECLARE @Id TABLE ([Action] VARCHAR(20), [Id]");
            merge.Append(identity.Type);
            merge.Append(')');
        }
        merge.Append($@"
MERGE [{tableName}] AS T  
USING [{tempTableName}] AS S
ON ({string.Join(" AND ", primaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN NOT MATCHED
THEN INSERT ({columnsString}) VALUES ({columnsString})
WHEN MATCHED 
THEN UPDATE SET {string.Join(',', columnNames.Except(primaryKeys).Select(x => $"T.[{x}] = S.[{x}]"))}");
        if (identityExist)
        {
            merge.AppendLine();
            merge.Append($"OUTPUT $action, inserted.");
            merge.Append(identity.ColumnName);
            merge.AppendLine(" INTO @Id ([Action], [Id]);");
            merge.AppendLine("SELECT [Id] FROM @Id WHERE [Action] = 'INSERT' ORDER BY [Id] ASC");
        }
        else
        {
            merge.AppendLine(";");
        }
        merge.AppendLine($"DROP TABLE {tempTableName}");
        return merge.ToString();
    }

    public string GetAlterIdentityColumnQuery(string tempTableName, Identity identity)
        => $@"ALTER TABLE {tempTableName} ADD _IfIdentityJustOneColumn_{identity.ColumnName} BIT
ALTER TABLE {tempTableName} DROP COLUMN {identity.ColumnName}
ALTER TABLE {tempTableName} ADD {identity.ColumnName} {identity.Type}
ALTER TABLE {tempTableName} DROP COLUMN _IfIdentityJustOneColumn_{identity.ColumnName}";

    public string GetInsertQuery(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity)
    {
        var identityExist = identity is not null;
        var columnsString = string.Join(',', (!identityExist ? columnNames : columnNames.Where(x => x != identity.ColumnName)).Select(x => $"[{x}]"));
        var insert = new StringBuilder();
        if (identityExist)
        {
            insert.Append($"DECLARE @Id TABLE ([Id] {identity.Type})");
        }

        insert.Append($@"INSERT INTO [{tableName}]({columnsString})");
        if (identityExist)
        {
            insert.AppendLine($"OUTPUT inserted.[{identity.ColumnName}] INTO @Id");
        }
        insert.AppendLine($"SELECT {columnsString} FROM {tempTableName}");
        if (identityExist)
        {
            insert.AppendLine("SELECT [Id] FROM @Id ORDER BY [Id] ASC");
        }
        insert.AppendLine($"DROP TABLE {tempTableName}");
        return insert.ToString();
    }

    public string GetUpdateQuery(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity) => $@"
MERGE [{tableName}] AS T  
USING (SELECT * FROM [{tempTableName}]) AS S
ON ({string.Join(" AND ", primaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN MATCHED 
THEN UPDATE SET {string.Join(',', columnNames.Except(primaryKeys).Select(x => $"T.[{x}] = S.[{x}]"))};
DROP TABLE {tempTableName}";

    public string GetDeleteQuery(string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity) => $@"
MERGE [{tableName}] AS T  
USING (SELECT * FROM [{tempTableName}]) AS S
ON ({string.Join(" AND ", primaryKeys.Select(x => $"S.[{x}] = T.[{x}]"))})
WHEN MATCHED
THEN DELETE;
DROP TABLE {tempTableName}";

    public string GetTempTableName(string targetTableName) => $"#{targetTableName}";

    public IDbDataParameter CreateParameter(object value) => new SqlParameter { Value = value};

}