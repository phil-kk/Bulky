using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using BulkyMerge.Root;
using Npgsql;

namespace BulkyMerge.PostgreSql.PostgreSql;

public sealed class NpgsqlDialect : ISqlDialect
{
    public string DefaultScheme => "public";

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

    public string GetInsertOrUpdateMergeStatement(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity)
    {
        var identityExist = identity is not null;
        columnNames = identityExist ? columnNames.Where(x => x != identity.ColumnName) : columnNames;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', columnNames.Select(x => $"\"{x}\""));
        var primaryKeysMatchString =
            string.Join(" AND ", primaryKeys.Select(x => $"d.\"{x}\" = s.\"{x}\""));
        merge.Append($@"WITH ""updated"" AS(UPDATE ""{tableName}"" AS d 
        SET {string.Join(',', columnNames.Except(primaryKeys).Select(x => $"\"{x}\" = s.\"{x}\""))}
        FROM ""{tempTableName}"" AS s
        WHERE {primaryKeysMatchString}
        RETURNING {string.Join(',', primaryKeys.Select(x => $"d.\"{x}\""))}
            )
DELETE 
FROM ""{tempTableName}"" d
     USING ""updated"" s WHERE {primaryKeysMatchString};");
        var insertClause = @$"INSERT INTO ""{tableName}"" ({columnsString})
        SELECT {columnsString} FROM ""{tempTableName}""";
        if (identityExist)
        {
            merge.Append(@$"WITH ""inserted"" AS ({insertClause}
        RETURNING ""{identity.ColumnName}"")
                SELECT ""{identity.ColumnName}"" FROM ""inserted"";
        DROP TABLE ""{tempTableName}""");
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

    public string GetInsertQuery(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity)
    {
        var identityExist = identity is not null;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', (!identityExist ? columnNames : columnNames.Where(x => x != identity.ColumnName)).Select(x => $"\"{x}\""));
        var insertClause = @$"INSERT INTO ""{tableName}"" ({columnsString})
        SELECT {columnsString} FROM ""{tempTableName}""";
        if (identityExist)
        {
            merge.Append(@$"WITH ""inserted"" AS ({insertClause}
        RETURNING ""{identity.ColumnName}"")
                SELECT ""{identity.ColumnName}"" FROM ""inserted"";
        DROP TABLE ""{tempTableName}""");
        }
        else
        {
            merge.Append($"{insertClause};DROP TABLE \"{tempTableName}\"");
        }

        return merge.ToString();
    }

    public string GetUpdateQuery(IEnumerable<string> columnNames, string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity)
        => @$"UPDATE ""{tableName}"" AS d 
        SET {string.Join(',', columnNames.Except(primaryKeys).Select(x => $"\"{x}\" = s.\"{x}\""))}
FROM ""{tempTableName}"" AS s
WHERE {string.Join(" AND ", primaryKeys.Select(x => $"d.\"{x}\" = s.\"{x}\""))};
DROP TABLE ""{tempTableName}""";

    public string GetDeleteQuery(string tableName, string tempTableName, IEnumerable<string> primaryKeys, Identity identity)
        => $@"
DELETE 
FROM ""{tableName}"" s
     USING ""{tempTableName}"" t WHERE {string.Join(" AND ", primaryKeys.Select(x => @$"s.""{x}"" = t.""{x}"""))};
DROP TABLE ""{tempTableName}""";

    public string GetTempTableName(string targetTableName) => $"{targetTableName}_{Guid.NewGuid():N}";

    public IDbDataParameter CreateParameter(object value) => new NpgsqlParameter { Value = value };
}