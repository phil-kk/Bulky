using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using FastBulkOperations.Root;
using MySqlConnector;

namespace Dapper.FastBulkOperations.MySql;

internal static class WithLoveToMySqlExtensions
{
    internal static string CutTempTableName(this string tempTableName) => tempTableName.Length >= 63 ? tempTableName[..63] : tempTableName;
}
public sealed class MySqlDialect : ISqlDialect
{
    public string GetFindPrimaryKeysQuery(string databaseName, string tableName)
        => $@"SELECT COLUMN_NAME
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = '{databaseName}' AND TABLE_NAME = '{tableName}' AND COLUMN_KEY = 'PRI';";

    public string GetFindIdentityQuery(string databaseName, string tableName)
        => $@"SELECT COLUMN_NAME, COLUMN_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{databaseName}' AND TABLE_NAME = '{tableName}'
    AND EXTRA = 'auto_increment'";

    public string GetCreateTempTableQuery(string tempTableName, string destination, IEnumerable<string> columnNames = null) => $"CREATE TABLE IF NOT EXISTS `{tempTableName.CutTempTableName()}` AS (SELECT * FROM `{destination}` WHERE 1=0);";

    public string GetInsertOrUpdateMergeStatement(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
    {
        var identityExist = bulkWriteContext.Identity is not null;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', columnNames.Select(x => $"`{x}`"));
        var primaryKeysMatchString =
            string.Join(" AND ", bulkWriteContext.PrimaryKeys.Select(x => $"d.`{x}` = s.`{x}`"));
        var insertOrUpdateClause = @$"INSERT INTO `{bulkWriteContext.TableName}` ({columnsString}) SELECT {columnsString} FROM `{bulkWriteContext.TempTable.CutTempTableName()}`
        ON DUPLICATE KEY UPDATE {string.Join(',', columnNames.Except(bulkWriteContext.PrimaryKeys).Select(x => $"`{bulkWriteContext.TableName}`.`{x}` = `{bulkWriteContext.TempTable.CutTempTableName()}`.`{x}`"))};";
        if (identityExist)
        {
            merge.Append($"LOCK TABLES {bulkWriteContext.TempTable} READ, {bulkWriteContext.TableName} WRITE;");
            merge.Append(insertOrUpdateClause);
            merge.Append($@"SET @row_count = ROW_COUNT();
SET @last_insert_id = LAST_INSERT_ID();
UNLOCK TABLES;
SELECT `{bulkWriteContext.Identity.ColumnName}` FROM `{bulkWriteContext.TableName}` WHERE `{bulkWriteContext.Identity.ColumnName}` >= @last_insert_id AND `{bulkWriteContext.Identity.ColumnName}` <= @last_insert_id + (@row_count - 1);
DROP TABLE `{bulkWriteContext.TempTable.CutTempTableName()}`");
        }
        else
        {
            merge.Append($"{insertOrUpdateClause}DROP TABLE `{bulkWriteContext.TempTable.CutTempTableName()}`");
        } 
       
        return merge.ToString();
    }

    public string GetAlterIdentityColumnQuery(string tempTableName, Identity identity)
        => $@"ALTER TABLE `{tempTableName.CutTempTableName()}` DROP COLUMN `{identity.ColumnName}`;
ALTER TABLE `{tempTableName.CutTempTableName()}` ADD `{identity.ColumnName}` {identity.Type}";

    public string GetInsertQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
    {
        var identityExist = bulkWriteContext.Identity is not null;
        var merge = new StringBuilder();
        var columnsString = string.Join(',', (!identityExist ? columnNames : columnNames.Where(x => x != bulkWriteContext.Identity.ColumnName)).Select(x => $"`{x}`"));
        var insertClause = @$"INSERT INTO `{bulkWriteContext.TableName}` ({columnsString})
        SELECT {columnsString} FROM `{bulkWriteContext.TempTable.CutTempTableName()}`;";
        if (identityExist)
        {
            merge.Append($"LOCK TABLES {bulkWriteContext.TempTable} READ, {bulkWriteContext.TableName} WRITE;");
            merge.Append(insertClause);
            merge.Append($@"SET @row_count = ROW_COUNT();
SET @last_insert_id = LAST_INSERT_ID();
UNLOCK TABLES;
SELECT `{bulkWriteContext.Identity.ColumnName}` FROM `{bulkWriteContext.TableName}` WHERE `{bulkWriteContext.Identity.ColumnName}` >= @last_insert_id AND `{bulkWriteContext.Identity.ColumnName}` <= @last_insert_id + (@row_count - 1);
DROP TABLE `{bulkWriteContext.TempTable.CutTempTableName()}`");
        }
        else
        {
            merge.Append($"{insertClause}DROP TABLE `{bulkWriteContext.TempTable.CutTempTableName()}`");
        }

        return merge.ToString();
    }

    public string GetUpdateQuery(IEnumerable<string> columnNames, BulkWriteContext bulkWriteContext)
    {
        var primaryKeysMatchString =
            string.Join(" AND ", bulkWriteContext.PrimaryKeys.Select(x => $"`{bulkWriteContext.TableName}`.`{x}` = `{bulkWriteContext.TempTable}`.`{x}`"));
        return @$"UPDATE `{bulkWriteContext.TableName}` INNER JOIN `{bulkWriteContext.TempTable.CutTempTableName()}`ON ({primaryKeysMatchString})
        SET {string.Join(',', columnNames.Except(bulkWriteContext.PrimaryKeys).Select(x => $"`{bulkWriteContext.TableName}`.`{x}` = `{bulkWriteContext.TempTable.CutTempTableName()}`.`{x}`"))}
;DROP TABLE `{bulkWriteContext.TempTable.CutTempTableName()}`";;
    }
    
    public string GetDeleteQuery(BulkWriteContext createTempTableResult)
        => $@"DELETE `{createTempTableResult.TableName}`
FROM `{createTempTableResult.TableName}`
INNER JOIN `{createTempTableResult.TempTable.CutTempTableName()}` ON ({string.Join(" AND ", createTempTableResult.PrimaryKeys.Select(x => @$"`{createTempTableResult.TableName}`.`{x}` = `{createTempTableResult.TempTable.CutTempTableName()}`.`{x}`"))});
DROP TABLE `{createTempTableResult.TempTable.CutTempTableName()}`";

    public string GetTempTableName(string targetTableName) => $"{targetTableName}_{Guid.NewGuid():N}".CutTempTableName();

    public IDbDataParameter CreateParameter(object value) => new MySqlParameter { Value = value };
}