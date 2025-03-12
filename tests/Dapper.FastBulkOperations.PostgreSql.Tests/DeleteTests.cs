using Bulky.PostgreSql;
using Dapper;
using Npgsql;

namespace Bulky.PostgreSql.Tests;

public class DeleteTests : PgSqlTestsBase
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
                new()
                {
                    Id = id1
                },
                new()
                {
                    Id = id2
                }
            };
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            if (sync)
            {
                connection.BulkDelete(items, tableName);
            }
            else
            {
                await connection.BulkDeleteAsync(items, tableName);
            }
            var select = await connection.QueryAsync<AllFieldTypesWithIdentityTests>($"SELECT * FROM \"{tableName}\" ORDER BY \"Id\" ASC");
            Assert.True(select.Count() is 0);
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
        var tableName = $"AttributeBasedMapping_Test_{Guid.NewGuid():N}";
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
                SecondKey = key1
            });
            items.Add(new AttributeBasedMapping
            {
                FirstKey = key2,
                SecondKey = key2
            });
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            if (sync)
            {
                connection.BulkDelete(items, tableName);
            }
            else
            {
                await connection.BulkDeleteAsync(items, tableName);
            }
            var select = connection.Query<AttributeBasedMapping>($"SELECT \"FirstKey\", \"SecondKey\", \"Field\" as \"FieldToUpdate\" {(createNotMappedField ? ",\"NotMapped\"" : string.Empty)} FROM \"{tableName}\"");

            Assert.True(select.Count() is 0);
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
    public async Task Should_Pass_When_Parameters_Based_Mappings_Is_Used(bool createNotMappedField, bool sync)
    {
        var tableName = $"PK_Test_Update_{Guid.NewGuid():N}";
        try
        {
            CreateCustomPrimaryKeysTableWithNotMapped(tableName);
            var items = new List<CustomPrimaryKeyFieldsParametersMappings>();
            var key1 = Guid.NewGuid();
            var key2 = Guid.NewGuid();
            InsertCustomPrimaryKeyFieldsParametersMappings(tableName, new CustomPrimaryKeyFieldsParametersMappings()
            {
                FirstKey = key1,
                SecondKey = key1,
                FieldToUpdate = "TO_UPDATE_1",
                Exclude = "NOT_MAPPED"
            }, createNotMappedField);
            InsertCustomPrimaryKeyFieldsParametersMappings(tableName, new CustomPrimaryKeyFieldsParametersMappings
            {
                FirstKey = key2,
                SecondKey = key2,
                FieldToUpdate = "TO_UPDATE_2",
                Exclude = "NOT_MAPPED"
            }, createNotMappedField);
            
            items.Add(new CustomPrimaryKeyFieldsParametersMappings
            {
                FirstKey = key1
            });
            items.Add(new CustomPrimaryKeyFieldsParametersMappings
            {
                FirstKey = key2
            });
            await using var connection = new NpgsqlConnection(ConnectionString);
            await connection.OpenAsync();
            if (sync)
            {
                connection.BulkDelete(items, tableName, primaryKeys:new [] {"FirstKey"});
            }
            else
            {
                await connection.BulkDeleteAsync(items, tableName, primaryKeys:new [] {"FirstKey"});
            }
            var select = connection.Query<CustomPrimaryKeyFieldsParametersMappings>($"SELECT \"FirstKey\", \"SecondKey\", \"FieldToUpdate\" {(createNotMappedField ? ",\"Exclude\"" : string.Empty)} FROM \"{tableName}\"");

            Assert.True(select.Count() is 0);
        }
        finally
        {
            DropTable(tableName);
        }
    }
}