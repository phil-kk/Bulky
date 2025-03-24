using BulkyMerge.PostgreSql;
using Dapper;
using Npgsql;

namespace BulkyMerge.PostgreSql.Tests;

public class InsertOrUpdateTests: PgSqlTestsBase
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Should_Pass_When_All_Inserted_Fields_Are_Valid(bool sync)
    {
        var tableName = $"AllFieldTypesTests_{Guid.NewGuid():N}";
        try
        {
            CreateAllFieldsTable(tableName);
            var id1 = InsertAllFields(tableName,new  AllFieldTypesWithIdentityTests
            {
                DecimalValue = -1,
                IntValue = -1
            });
            var id2 = InsertAllFields(tableName, new AllFieldTypesWithIdentityTests
            {
                DecimalValue = -2,
                IntValue = -2
            });
            var items = new List<AllFieldTypesWithIdentityTests>
            {
                new AllFieldTypesWithIdentityTests
                {
                    Id = id1,
                    DecimalValue = 0,
                    IntValue = 0
                },
                new AllFieldTypesWithIdentityTests
                {
                    Id = id2, 
                    IntValue = 1
                }
            };
            for (var i = 0; i < 2; i++)
            {
                items.Add(new AllFieldTypesWithIdentityTests
                {
                    DecimalValue = 2 + i,
                    IntValue = 2 + i
                });
            }
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            if (sync)
            {
                connection.BulkInsertOrUpdate(items, tableName);
            }
            else
            {
                await connection.BulkInsertOrUpdateAsync(items, tableName);
            }
            var select = await connection.QueryAsync<AllFieldTypesWithIdentityTests>($"SELECT * FROM \"{tableName}\" ORDER BY \"Id\" ASC");
            AllFieldsTestAssertions(select, items);
        }
        catch (Exception e)
        {
            ;
        }
        finally
        {
            DropTable(tableName);
        }
    }
    
    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(false, false)]
    public async Task Should_Pass_When_Attribute_Based_Mappings_Is_Used(bool createNotMappedField, bool sync)
    {
        var keys = Enumerable.Range(0, 100).Select(x => Guid.NewGuid()).OrderBy(x => x).ToList();
        var tableName = $"AttributeBasedMapping_Test";
        try
        {
            CreateAttributeBasedMappingTableWithNotMapped(tableName);
            var items = new List<AttributeBasedMapping>();
            var key1 = Guid.NewGuid();
            var key2 = Guid.NewGuid();
            InsertAttributeBasedMapping(tableName, new AttributeBasedMapping
            {
                FirstKey = key1,
                SecondKey = key1,
                FieldToUpdate = "TO_UPDATE_1",
                NotMapped = "NOT_MAPPED"
            }, createNotMappedField);
            InsertAttributeBasedMapping(tableName, new AttributeBasedMapping
            {
                FirstKey = key2,
                SecondKey = key2,
                FieldToUpdate = "TO_UPDATE_2",
                NotMapped = "NOT_MAPPED"
            }, createNotMappedField);
            
            items.Add(new AttributeBasedMapping
            {
                FirstKey = key1,
                SecondKey = key1,
                FieldToUpdate = "UPDATED_0",
                NotMapped = "SHOULD_NOT_BE_UPDATED_0"
            });
            items.Add(new AttributeBasedMapping
            {
                FirstKey = key2,
                SecondKey = key2,
                FieldToUpdate = "UPDATED_1",
                NotMapped = "SHOULD_NOT_BE_UPDATED_1"
            });
            for (var i = 0; i < 100; i++)
            {
                items.Add(new AttributeBasedMapping
                {
                    FirstKey = keys[i],
                    SecondKey = keys[i],
                    FieldToUpdate = $"Test {i}",
                    NotMapped = "test"
                });
            }
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            if (sync)
            {
                connection.BulkInsertOrUpdate(items);
            }
            else
            {
                await connection.BulkInsertOrUpdateAsync(items);
            }
            var select = connection.Query<AttributeBasedMapping>($"SELECT \"FirstKey\", \"SecondKey\", \"Field\" as \"FieldToUpdate\" {(createNotMappedField ? ",\"NotMapped\"" : string.Empty)} FROM \"{tableName}\"");

            Assert.Equal(select.Count(), items.Count);
            Assert.True(select.Count(x => x.NotMapped == null) >= 100);
            Assert.True(select.Count(x => x.FieldToUpdate.StartsWith("UPDATED_")) == 2);
            Assert.True(select.Any(x => x.SecondKey == key1 && x.FirstKey == key1));
            Assert.True(select.Any(x => x.SecondKey == key2 && x.FirstKey == key2));
        }
        finally
        {
            DropTable(tableName);
        }
    }

    
    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(false, false)]
    public async Task Should_Pass_When_Property_Mappings_Is_Used(bool createExcludeColumn, bool sync)
    {
        var keys = Enumerable.Range(0, 100).Select(x => Guid.NewGuid()).OrderBy(x => x).ToList();
        var tableName = $"PK_IU_Test";
        try
        {
            CreateCustomPrimaryKeysTableWithNotMapped(tableName);
            var items = new List<CustomPrimaryKeyFieldsParametersMappings>();
            var key1 = Guid.NewGuid();
            var key2 = Guid.NewGuid();
            InsertCustomPrimaryKeyFieldsParametersMappings(tableName, new CustomPrimaryKeyFieldsParametersMappings
            {
                FirstKey = key1,
                SecondKey = key1,
                FieldToUpdate = "TO_UPDATE_1",
                Exclude = "NOT_MAPPED"
            }, createExcludeColumn);
            InsertCustomPrimaryKeyFieldsParametersMappings(tableName, new CustomPrimaryKeyFieldsParametersMappings
            {
                FirstKey = key2,
                SecondKey = key2,
                FieldToUpdate = "TO_UPDATE_2",
                Exclude = "NOT_MAPPED"
            }, createExcludeColumn);
            
            items.Add(new CustomPrimaryKeyFieldsParametersMappings
            {
                FirstKey = key1,
                SecondKey = key1,
                FieldToUpdate = "UPDATED_0",
                Exclude = "SHOULD_NOT_BE_UPDATED_0"
            });
            items.Add(new CustomPrimaryKeyFieldsParametersMappings
            {
                FirstKey = key2,
                SecondKey = key2,
                FieldToUpdate = "UPDATED_1",
                Exclude = "SHOULD_NOT_BE_UPDATED_1"
            });
            for (var i = 0; i < 100; i++)
            {
                items.Add(new CustomPrimaryKeyFieldsParametersMappings
                {
                    FirstKey = keys[i],
                    SecondKey = keys[i],
                    FieldToUpdate = $"Test {i}",
                    Exclude = "test"
                });
            }
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            if (sync)
            {
                connection.BulkInsertOrUpdate(items, tableName, excludeProperties:new [] {"Exclude"}, primaryKeys:new []{"FirstKey"});
            }
            else
            {
                await connection.BulkInsertOrUpdateAsync(items, tableName, excludeProperties:new [] {"Exclude"}, primaryKeys:new []{"FirstKey"});
            }
            var select = connection.Query<CustomPrimaryKeyFieldsParametersMappings>($"SELECT \"FirstKey\", \"SecondKey\", \"FieldToUpdate\" {(createExcludeColumn ? ",\"Exclude\"" : string.Empty)} FROM \"{tableName}\"");

            Assert.Equal(select.Count(), items.Count);
                //Assert.True(select.Count(x => x.Exclude == (createExcludeColumn ? string.Empty : null)) >= 100);
            Assert.True(select.Count(x => x.FieldToUpdate.StartsWith("UPDATED_")) == 2);
            Assert.True(select.Any(x => x.SecondKey == key1 && x.FirstKey == key1));
            Assert.True(select.Any(x => x.SecondKey == key2 && x.FirstKey == key2));
        }
        finally
        {
            DropTable(tableName);
        }
    }
}