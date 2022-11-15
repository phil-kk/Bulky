using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using FastBulkOperations.Root;
using Npgsql;

namespace Dapper.FastBulkOperations.PostgreSql;

public sealed class NpgsqlDialect : ISqlDialect
{
    public string GetFindPrimaryKeysQuery(string databaseName, string tableName)
        => $@"SELECT       
    pg_attribute.attname
    FROM pg_catalog.pg_index, pg_catalog.pg_class, pg_catalog.pg_attribute, pg_catalog.pg_namespace 
        WHERE 
    indrelid = pg_class.oid AND 
    nspname = 'public' AND 
        pg_class.relnamespace = pg_namespace.oid AND 
    pg_class.relname = '{tableName}' AND
        pg_attribute.attrelid = pg_class.oid AND 
    pg_attribute.attnum = any(pg_index.indkey)
    AND indisprimary";

    public string GetFindIdentityQuery(string databaseName, string tableName)
        => @$"SELECT
    pg_attribute.attname as ""ColumnName"",
    format_type(pg_attribute.atttypid, pg_attribute.atttypmod) as ""Type""
    FROM
        pg_catalog.pg_attribute
    INNER JOIN
    pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
        INNER JOIN
        pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE
    pg_attribute.attnum > 0
    AND NOT pg_attribute.attisdropped
        AND pg_namespace.nspname = 'public'
    AND pg_class.relname = '{tableName}'
    AND pg_attribute.attidentity = 'a'
    ORDER BY
    attnum ASC LIMIT 1";

    public string GetCreateTempTableQuery(string tempTableName, string destination, IEnumerable<string> columnNames = null) => $"SELECT * INTO TEMP \"{tempTableName}\" FROM \"{destination}\" WHERE 1 = 0;";

    public string GetInsertOrUpdateMergeStatement(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
    {
        var identityExist = bulkWriteContext.Identity is not null;
        columnNames = identityExist ? columnNames.Where(x => x != bulkWriteContext.Identity.ColumnName) : columnNames;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', columnNames.Select(x => $"\"{x}\""));
        var primaryKeysMatchString =
            string.Join(" AND ", bulkWriteContext.PrimaryKeys.Select(x => $"d.\"{x}\" = s.\"{x}\""));
        merge.Append($@"WITH ""updated"" AS(UPDATE ""{bulkWriteContext.TableName}"" AS d 
        SET {string.Join(',', columnNames.Except(bulkWriteContext.PrimaryKeys).Select(x => $"\"{x}\" = s.\"{x}\""))}
        FROM ""{bulkWriteContext.TempTable}"" AS s
        WHERE {primaryKeysMatchString}
        RETURNING {string.Join(',', bulkWriteContext.PrimaryKeys.Select(x => $"d.\"{x}\""))}
            )
        MERGE INTO ""{bulkWriteContext.TempTable}"" d
            USING ""updated"" s
            ON {primaryKeysMatchString}
        WHEN MATCHED THEN
            DELETE;");
        var insertClause = @$"INSERT INTO ""{bulkWriteContext.TableName}"" ({columnsString})
        SELECT {columnsString} FROM ""{bulkWriteContext.TempTable}""";
        if (identityExist)
        {
            merge.Append(@$"WITH ""inserted"" AS ({insertClause}
        RETURNING ""{bulkWriteContext.Identity.ColumnName}"")
                SELECT ""{bulkWriteContext.Identity.ColumnName}"" FROM ""inserted"";
        DROP TABLE ""{bulkWriteContext.TempTable}""");
        }
        else
        {
            merge.Append(insertClause);
        }
       
        return merge.ToString();
    }

    public string GetAlterIdentityColumnQuery(string tempTableName, Identity identity)
        => $@"ALTER TABLE ""{tempTableName}"" DROP COLUMN ""{identity.ColumnName}"";
ALTER TABLE ""{tempTableName}"" ADD ""{identity.ColumnName}"" {identity.Type}";

    public string GetInsertQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
    {
        var identityExist = bulkWriteContext.Identity is not null;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', (!identityExist ? columnNames : columnNames.Where(x => x != bulkWriteContext.Identity.ColumnName)).Select(x => $"\"{x}\""));
        var insertClause = @$"INSERT INTO ""{bulkWriteContext.TableName}"" ({columnsString})
        SELECT {columnsString} FROM ""{bulkWriteContext.TempTable}""";
        if (identityExist)
        {
            merge.Append(@$"WITH ""inserted"" AS ({insertClause}
        RETURNING ""{bulkWriteContext.Identity.ColumnName}"")
                SELECT ""{bulkWriteContext.Identity.ColumnName}"" FROM ""inserted"";
        DROP TABLE ""{bulkWriteContext.TempTable}""");
        }
        else
        {
            merge.Append($"{insertClause};DROP TABLE \"{bulkWriteContext.TempTable}\"");
        }

        return merge.ToString();
    }

    public string GetUpdateQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
        => @$"UPDATE ""{bulkWriteContext.TableName}"" AS d 
        SET {string.Join(',', columnNames.Except(bulkWriteContext.PrimaryKeys).Select(x => $"\"{x}\" = s.\"{x}\""))}
FROM ""{bulkWriteContext.TempTable}"" AS s
WHERE {string.Join(" AND ", bulkWriteContext.PrimaryKeys.Select(x => $"d.\"{x}\" = s.\"{x}\""))};
DROP TABLE ""{bulkWriteContext.TempTable}""";

    public string GetDeleteQuery(BulkWriteContext createTempTableResult)
        => $@"
MERGE INTO ""{createTempTableResult.TableName}"" AS T  
USING (SELECT * FROM ""{createTempTableResult.TempTable}"") AS S
ON ({string.Join(" AND ", createTempTableResult.PrimaryKeys.Select(x => @$"S.""{x}"" = T.""{x}"""))})
WHEN MATCHED
THEN DELETE;
DROP TABLE ""{createTempTableResult.TempTable}""";

    public string GetTempTableName(string targetTableName) => $"{targetTableName}_{Guid.NewGuid():N}";

    public IDbDataParameter CreateParameter(object value) => new NpgsqlParameter { Value = value };
}