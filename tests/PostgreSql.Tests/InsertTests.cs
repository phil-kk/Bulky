using BulkyMerge.PostgreSql;
using BulkyMerge.Root;
using Dapper;
using FastMember;
using Microsoft.VisualStudio.TestPlatform.ObjectModel;
using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
namespace BulkyMerge.PostgreSql.Tests
{
    public class InsertTests : PgSqlTestsBase
    {
        /* List<AllFieldTypesWithIdentityTests> Generate(int count = 10_000)
         {
             return Enumerable.Range(0, count).Select(x => new AllFieldTypesWithIdentityTests
             {
                 VarcharValue = $"Test {x}",
                 TextValue = $"Some long text {x}",
                 CharValue = (char)('A' + (x % 26)),
                 BooleanValue = x % 2 == 0,
                 SmallIntValue = (short)(x % short.MaxValue),
                 IntValue = x,
                 BigIntValue = x * 1000L,
                 DecimalValue = (decimal)(x * 0.1),

                 RealValue = (float)(x * 0.1),
                 DoublePrecisionValue = x * 0.0001,

                 DateValue = count % 2 == 0 ? DateTime.UtcNow.AddDays(-x) : null,
                 TimestampValue = DateTime.UtcNow,
                 TimeValue = TimeSpan.FromSeconds(x % 86400),
                 TimestampTzValue = DateTimeOffset.UtcNow.AddSeconds(-x),
             }).ToList();
         }

         public class CommonJsonConvertTypeHandler : ITypeConverter
         {
             public object Convert(object value)
             {
                 return JsonConvert.SerializeObject(value);
             }
         }
         [Fact]
         public async Task Test2()
         {
             try
             {
                 TypeConverters.RegisterTypeConverter(typeof(List<JsontTest>), new CommonJsonConvertTypeHandler());
                 var list = Generate(1000);
                 CreateAllFieldsTable(nameof(AllFieldTypesWithIdentityTests));

                 var stopwatch = Stopwatch.StartNew();
                 using var connection = new NpgsqlConnection(ConnectionString);
                 connection.Open();
                 await connection.BulkInsertAsync(list);
                 //await (connection as IDbConnection).BulkInsertAsync(list);

                 var elapsed = stopwatch.Elapsed;
                 ;
             }
             catch (Exception e)
             {
                 ;
             }

         }*/

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Should_Pass_When_All_Inserted_Fields_Are_Valid(bool sync)
        {
            var tableName = $"AllFieldTypesTests_{Guid.NewGuid():N}";
            try
            {
                CreateAllFieldsTable(tableName);
                var items = new List<AllFieldTypesWithIdentityTests>();
                for (var i = 0; i < 100; i++)
                {
                    items.Add(new AllFieldTypesWithIdentityTests
                    {
                        DecimalValue = i,
                        IntValue = i
                    });
                }
                await using var connection = new NpgsqlConnection(ConnectionString);
                await connection.OpenAsync();
                if (sync)
                {
                    connection.BulkInsert(items, tableName);
                }
                else
                {
                    await connection.BulkInsertAsync(items, tableName);
                }
                var select = await connection.QueryAsync<AllFieldTypesWithIdentityTests>($"SELECT * FROM \"{tableName}\" ORDER BY \"Id\" ASC");
                AllFieldsTestAssertions(select, items);
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
            var tableName = $"AttributeBasedMapping_Test_{Guid.NewGuid():N}";
            try
            {
                CreateAttributeBasedMappingTableWithNotMapped(tableName);
                var items = new List<AttributeBasedMapping>();
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
                    connection.BulkInsert(items, tableName);
                }
                else
                {
                    await connection.BulkInsertAsync(items, tableName);
                }
                var select = connection.Query<AttributeBasedMapping>($"SELECT \"FirstKey\", \"SecondKey\", \"Field\" as \"FieldToUpdate\" {(createNotMappedField ? ",\"NotMapped\"" : string.Empty)} FROM \"{tableName}\"");

                Assert.Equal(select.Count(), items.Count);
                Assert.True(select.All(x => x.NotMapped == null));
                Assert.True(select.OrderBy(x => x.FirstKey).Select(x => x.FirstKey).SequenceEqual(keys));
                Assert.True(select.OrderBy(x => x.SecondKey).Select(x => x.SecondKey).SequenceEqual(keys));
                Assert.True(select.OrderBy(x => x.FieldToUpdate).Select(x => x.FieldToUpdate).SequenceEqual(Enumerable.Range(0, 100).Select(x => $"Test {x}").OrderBy(x => x)));

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
            var tableName = $"PrimaryKeyFieldParametersMappings_Test";
            try
            {
                CreateCustomPrimaryKeysTableWithNotMapped(tableName);
                var items = new List<CustomPrimaryKeyFieldsParametersMappings>();
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
                    connection.BulkInsert(items, tableName:tableName, primaryKeys:new [] { "FirstKey", "SecondKey"}, excludeProperties:new string[] { "Exclude"});
                }
                else
                {
                    await connection.BulkInsertAsync(items, tableName:tableName, primaryKeys:new [] { "FirstKey", "SecondKey"}, excludeProperties:new string[] { "Exclude"});
                }
                var select = connection.Query<CustomPrimaryKeyFieldsParametersMappings>($"SELECT \"FirstKey\", \"SecondKey\", \"FieldToUpdate\"{(createExcludeColumn ? ",\"Exclude\"" : string.Empty)} FROM \"{tableName}\"");

                Assert.Equal(select.Count(), items.Count);
                Assert.True(select.All(x => x.Exclude is null));
                Assert.True(select.OrderBy(x => x.FirstKey).Select(x => x.FirstKey).SequenceEqual(keys));
                Assert.True(select.OrderBy(x => x.SecondKey).Select(x => x.SecondKey).SequenceEqual(keys));
                Assert.True(select.OrderBy(x => x.FieldToUpdate).Select(x => x.FieldToUpdate).SequenceEqual(Enumerable.Range(0, 100).Select(x => $"Test {x}").OrderBy(x => x)));

            }
            finally
            {
                DropTable(tableName);
            }
        }
    }
}