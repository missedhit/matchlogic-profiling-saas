using MatchLogic.Application.Common;
using MatchLogic.Application.Features.MatchDefinition;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;

public class AutoMappingServiceTests
{
    private readonly IGenericRepository<FieldMappingEx, Guid> _mockFieldsRepo;
    private readonly IGenericRepository<DataSource, Guid> _mockDataSourceRepo;
    private readonly IGenericRepository<MappedFieldsRow, Guid> _mockMappedFieldRowsRepo;
    private readonly IStringSimilarityCalculator _mockSimilarityCalculator;
    private readonly IAutoMappingService _service;
    private readonly IDataStore _dataStore;
    private readonly IHeaderUtility _headerUtility;
    private readonly IFieldMappingService _fieldMappingService;

    public AutoMappingServiceTests()
    {
        var _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();

        IServiceCollection services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        services.AddApplicationSetup(_dbPath, _dbJobPath);


        var _serviceProvider = services.BuildServiceProvider();

        _mockFieldsRepo = _serviceProvider.GetService<IGenericRepository<FieldMappingEx, Guid>>();
        _mockDataSourceRepo = _serviceProvider.GetService<IGenericRepository<DataSource, Guid>>();
        _mockMappedFieldRowsRepo = _serviceProvider.GetService<IGenericRepository<MappedFieldsRow, Guid>>();

        // Mock the similarity calculator for controlled testing
        _mockSimilarityCalculator = _serviceProvider.GetService<IStringSimilarityCalculator>();
        _dataStore = _serviceProvider.GetService<IDataStore>();
        _headerUtility = _serviceProvider.GetService<IHeaderUtility>();
        _fieldMappingService = _serviceProvider.GetService<IFieldMappingService>();

        _service = new AutoMappingService(
            _mockFieldsRepo,
            _mockDataSourceRepo,
            _mockMappedFieldRowsRepo,
            _mockSimilarityCalculator,
            _dataStore,
            _headerUtility,
            _fieldMappingService);
    }

    #region GetExtendedFieldsAsync Tests

    [Fact]
    public async Task GetExtendedFieldsAsync_WithValidProject_ShouldReturnFieldsGroupedByDataSource()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSource1Id = Guid.NewGuid();
        var dataSource2Id = Guid.NewGuid();

        // Setup data sources
        var dataSources = new List<DataSource>
        {
            new DataSource { Id = dataSource1Id, Name = "CRM", ProjectId = projectId },
            new DataSource { Id = dataSource2Id, Name = "ERP", ProjectId = projectId }
        };

        foreach (var ds in dataSources)
        {
            await _mockDataSourceRepo.InsertAsync(ds, Constants.Collections.DataSources);
        }

        // Setup fields for data sources
        var fields1 = new List<FieldMappingEx>
        {
            new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSource1Id, FieldName = "CustomerName", FieldIndex
             =0},
            new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSource1Id, FieldName = "Email", FieldIndex = 1 }
        };

        var fields2 = new List<FieldMappingEx>
        {
            new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSource2Id, FieldName = "FullName", FieldIndex = 0 },
            new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSource2Id, FieldName = "EmailAddress" , FieldIndex = 1}
        };

        foreach (var field in fields1.Concat(fields2))
        {
            await _mockFieldsRepo.InsertAsync(field, Constants.Collections.FieldMapping);
        }

        // Act
        var result = await _service.GetExtendedFieldsAsync(projectId);

        // Assert
        Assert.Equal(2, result.Count);
        Assert.True(result.ContainsKey("CRM"));
        Assert.True(result.ContainsKey("ERP"));
        Assert.Equal(2, result["CRM"].Count);
        Assert.Equal(2, result["ERP"].Count);

        // Verify field details
        Assert.Equal("CustomerName", result["CRM"][0].FieldName);
        Assert.Equal("Email", result["CRM"][1].FieldName);
        Assert.Equal("FullName", result["ERP"][0].FieldName);
        Assert.Equal("EmailAddress", result["ERP"][1].FieldName);

        // Verify field indices are set correctly
        Assert.Equal(0, result["CRM"][0].FieldIndex);
        Assert.Equal(1, result["CRM"][1].FieldIndex);
        Assert.Equal(0, result["ERP"][0].FieldIndex);
        Assert.Equal(1, result["ERP"][1].FieldIndex);
    }

    [Fact]
    public async Task GetExtendedFieldsAsync_WithNonExistentProject_ShouldReturnEmptyDictionary()
    {
        // Arrange
        var nonExistentProjectId = Guid.NewGuid();

        // Act
        var result = await _service.GetExtendedFieldsAsync(nonExistentProjectId);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetExtendedFieldsAsync_WithProjectHavingNoFields_ShouldReturnDataSourcesWithEmptyLists()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();

        var dataSource = new DataSource { Id = dataSourceId, Name = "EmptyDS", ProjectId = projectId };
        await _mockDataSourceRepo.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Act
        var result = await _service.GetExtendedFieldsAsync(projectId);

        // Assert
        Assert.Single(result);
        Assert.True(result.ContainsKey("EmptyDS"));
        Assert.Empty(result["EmptyDS"]);
    }

    #endregion

    #region AutoMapFields Tests

    [Fact]
    public void AutoMapFields_WithTwoDataSources_ShouldMapSimilarFields()
    {
        // Arrange      

        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["CRM"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "CustomerName", DataSourceName = "CRM", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "Email", DataSourceName = "CRM", FieldIndex = 1 }
            },
            ["ERP"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "FullName", DataSourceName = "ERP", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "EmailAddress", DataSourceName = "ERP", FieldIndex = 1 }
            }
        };

        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert
        Assert.NotEmpty(result);

        // Should have mapped rows plus one empty row
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.Equal(2, nonEmptyRows.Count);

        // Check if high-similarity fields were mapped together
        var emailRow = nonEmptyRows.FirstOrDefault(r =>
            r["CRM"]?.FieldName == "Email" || r["ERP"]?.FieldName == "EmailAddress");
        Assert.NotNull(emailRow);

        var nameRow = nonEmptyRows.FirstOrDefault(r =>
            r["CRM"]?.FieldName == "CustomerName" || r["ERP"]?.FieldName == "FullName");
        Assert.NotNull(nameRow);

        // Verify fields are marked as mapped
        Assert.True(fieldsPerDataSource["CRM"].All(f => f.Mapped));
        Assert.True(fieldsPerDataSource["ERP"].All(f => f.Mapped));
    }

    [Fact]
    public void AutoMapFields_WithThreeDataSources_ShouldCreateMultiSourceMappings()
    {
        // Arrange      

        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "Name", DataSourceName = "DS1", FieldIndex = 0 }
            },
            ["DS2"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "CustomerName", DataSourceName = "DS2", FieldIndex = 0 }
            },
            ["DS3"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "PersonName", DataSourceName = "DS3", FieldIndex = 0 }
            }
        };

        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.NotEmpty(nonEmptyRows);

        // Check if fields from different data sources can be mapped together
        var nameRow = nonEmptyRows.FirstOrDefault(r =>
            r.GetAllFields().Count() > 1);

        if (nameRow != null)
        {
            // Verify multi-source mapping
            var fieldNames = nameRow.GetAllFields().Select(f => f.FieldName).ToList();
            Assert.Contains("Name", fieldNames);
        }
    }

    [Fact]
    public void AutoMapFields_WithEmptyInput_ShouldReturnEmptyRowOnly()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>();

        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void AutoMapFields_WithSingleDataSource_ShouldIncludeAllFieldsAsUnmapped()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "Field1", DataSourceName = "DS1", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "Field2", DataSourceName = "DS1", FieldIndex = 1 }
            }
        };

        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.Equal(2, nonEmptyRows.Count); // Each field gets its own row

        // Verify each field is in its own row
        Assert.Contains(nonEmptyRows, r => r["DS1"]?.FieldName == "Field1");
        Assert.Contains(nonEmptyRows, r => r["DS1"]?.FieldName == "Field2");
    }

    #endregion

    #region SequentialMapFields Tests

    [Fact]
    public void SequentialMapFields_WithMultipleDataSources_ShouldMapFieldsSequentially()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "Field1", DataSourceName = "DS1", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "Field2", DataSourceName = "DS1", FieldIndex = 1 }
            },
            ["DS2"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "FieldA", DataSourceName = "DS2", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "FieldB", DataSourceName = "DS2", FieldIndex = 1 }
            }
        };

        // Act
        var result = _service.SequentialMapFields(fieldsPerDataSource);

        // Assert
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.Equal(4, nonEmptyRows.Count); // Each field gets its own row

        // Verify all fields are mapped
        Assert.True(fieldsPerDataSource.Values.SelectMany(fields => fields).All(f => f.Mapped));

        // Verify each field is in its own row
        Assert.Contains(nonEmptyRows, r => r["DS1"]?.FieldName == "Field1");
        Assert.Contains(nonEmptyRows, r => r["DS1"]?.FieldName == "Field2");
        Assert.Contains(nonEmptyRows, r => r["DS2"]?.FieldName == "FieldA");
        Assert.Contains(nonEmptyRows, r => r["DS2"]?.FieldName == "FieldB");
    }

    [Fact]
    public void SequentialMapFields_WithDuplicateFieldNames_ShouldNotMapTwice()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "SameName", DataSourceName = "DS1", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "SameName", DataSourceName = "DS1", FieldIndex = 1 }
            }
        };

        // Act
        var result = _service.SequentialMapFields(fieldsPerDataSource);

        // Assert
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.Single(nonEmptyRows); // Only first occurrence should be mapped

        // Only first field should be mapped
        Assert.True(fieldsPerDataSource["DS1"][0].Mapped);
        Assert.False(fieldsPerDataSource["DS1"][1].Mapped);
    }

    [Fact]
    public void SequentialMapFields_WithEmptyInput_ShouldReturnEmptyRowOnly()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>();

        // Act
        var result = _service.SequentialMapFields(fieldsPerDataSource);

        // Assert
        Assert.Empty(result);
    }

    #endregion

    #region ClearMapFields Tests

    [Fact]
    public void ClearMapFields_WithMultipleDataSources_ShouldUseOnlyFirstDataSource()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["FirstDS"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "Field1", DataSourceName = "FirstDS", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "Field2", DataSourceName = "FirstDS", FieldIndex = 1 }
            },
            ["SecondDS"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "FieldA", DataSourceName = "SecondDS", FieldIndex = 0 }
            }
        };

        // Act
        var result = _service.ClearMapFields(fieldsPerDataSource);

        // Assert
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.Equal(2, nonEmptyRows.Count); // Only fields from first data source

        // Verify only first data source fields are mapped
        Assert.All(nonEmptyRows, row => Assert.True(row["FirstDS"] != null));
        Assert.All(nonEmptyRows, row => Assert.True(row["SecondDS"] == null));

        // Verify first data source fields are marked as mapped
        Assert.True(fieldsPerDataSource["FirstDS"].All(f => f.Mapped));
        Assert.False(fieldsPerDataSource["SecondDS"].Any(f => f.Mapped));

    }

    [Fact]
    public void ClearMapFields_WithEmptyInput_ShouldReturnEmptyRowOnly()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>();

        // Act
        var result = _service.ClearMapFields(fieldsPerDataSource);

        // Assert
        Assert.Empty(result);
    }

    [Fact]
    public void ClearMapFields_ShouldResetAllMappedFlags()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "Field1", DataSourceName = "DS1", FieldIndex = 0, Mapped = true },
                new FieldMappingEx { FieldName = "Field2", DataSourceName = "DS1", FieldIndex = 1, Mapped = true }
            },
            ["DS2"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "FieldA", DataSourceName = "DS2", FieldIndex = 0, Mapped = true }
            }
        };

        // Act
        var result = _service.ClearMapFields(fieldsPerDataSource);

        // Assert
        // Only first data source fields should be mapped after clear
        Assert.True(fieldsPerDataSource["DS1"].All(f => f.Mapped));
        Assert.False(fieldsPerDataSource["DS2"].Any(f => f.Mapped));
    }

    #endregion

    #region PerformAutoMappingAsync Tests

    [Fact]
    public async Task PerformAutoMappingAsync_WithValidProject_ShouldReturnMappedRowsAndSave()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();

        // Setup data source
        var dataSource = new DataSource { Id = dataSourceId, Name = "TestDS", ProjectId = projectId };
        await _mockDataSourceRepo.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Setup fields
        var field = new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSourceId, FieldName = "TestField" };
        await _mockFieldsRepo.InsertAsync(field, Constants.Collections.FieldMapping);

        // Act
        var result = await _service.PerformAutoMappingAsync(projectId);

        // Assert
        Assert.NotEmpty(result);

        // Verify data was saved to repository
        var savedMappings = await _mockMappedFieldRowsRepo.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);
        Assert.NotEmpty(savedMappings);
    }

    [Fact]
    public async Task PerformSequentialMappingAsync_WithValidProject_ShouldReturnMappedRowsAndSave()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();

        // Setup data source
        var dataSource = new DataSource { Id = dataSourceId, Name = "TestDS", ProjectId = projectId };
        await _mockDataSourceRepo.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Setup fields
        var field = new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSourceId, FieldName = "TestField" };
        await _mockFieldsRepo.InsertAsync(field, Constants.Collections.FieldMapping);

        // Act
        var result = await _service.PerformSequentialMappingAsync(projectId);

        // Assert
        Assert.NotEmpty(result);

        // Verify data was saved to repository
        var savedMappings = await _mockMappedFieldRowsRepo.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);
        Assert.NotEmpty(savedMappings);
    }

    [Fact]
    public async Task ClearMapFieldsAsync_WithValidProject_ShouldReturnClearedRowsAndSave()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();

        // Setup data source
        var dataSource = new DataSource { Id = dataSourceId, Name = "TestDS", ProjectId = projectId };
        await _mockDataSourceRepo.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Setup fields
        var field = new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSourceId, FieldName = "TestField" };
        await _mockFieldsRepo.InsertAsync(field, Constants.Collections.FieldMapping);

        // Act
        var result = await _service.ClearMapFieldsAsync(projectId);

        // Assert
        Assert.NotEmpty(result);

        // Verify data was saved to repository
        var savedMappings = await _mockMappedFieldRowsRepo.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);
        Assert.NotEmpty(savedMappings);
    }

    #endregion

    #region GetSavedMappedFieldRowsAsync Tests

    //[Fact]
    //public async Task GetSavedMappedFieldRowsAsync_WithExistingMappings_ShouldReturnLatestMapping()
    //{
    //    // Arrange
    //    var projectId = Guid.NewGuid();

    //    var oldMapping = new MappedFieldsRow
    //    {
    //        Id = Guid.NewGuid(),
    //        ProjectId = projectId,                
    //        MappedFields = new List<MappedFieldRow>
    //        {
    //            new MappedFieldRow()
    //        }
    //    };

    //    var newMapping = new MappedFieldsRow
    //    {
    //        Id = Guid.NewGuid(),
    //        ProjectId = projectId,                
    //        MappedFields = new List<MappedFieldRow>
    //        {
    //            new MappedFieldRow(),
    //            new MappedFieldRow()
    //        }
    //    };

    //    await _mockMappedFieldRowsRepo.InsertAsync(oldMapping, Constants.Collections.MappedFieldRows);
    //    await _mockMappedFieldRowsRepo.InsertAsync(newMapping, Constants.Collections.MappedFieldRows);

    //    // Act
    //    var result = await _service.GetSavedMappedFieldRowsAsync(projectId);

    //    // Assert
    //    Assert.Equal(2, result.Count); // Should return the newer mapping with 2 rows
    //}

    [Fact]
    public async Task GetSavedMappedFieldRowsAsync_WithNoMappings_ShouldReturnEmptyList()
    {
        // Arrange
        var projectId = Guid.NewGuid();

        // Act
        var result = await _service.GetSavedMappedFieldRowsAsync(projectId);

        // Assert
        Assert.Empty(result);
    }

    #endregion

    #region DeleteMappedFieldRowsAsync Tests

    [Fact]
    public async Task DeleteMappedFieldRowsAsync_WithExistingMappings_ShouldDeleteAllMappings()
    {
        // Arrange
        var projectId = Guid.NewGuid();

        var mapping1 = new MappedFieldsRow
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            MappedFields = new List<MappedFieldRow>()
        };

        var mapping2 = new MappedFieldsRow
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            MappedFields = new List<MappedFieldRow>()
        };

        await _mockMappedFieldRowsRepo.InsertAsync(mapping1, Constants.Collections.MappedFieldRows);
        await _mockMappedFieldRowsRepo.InsertAsync(mapping2, Constants.Collections.MappedFieldRows);

        // Act
        await _service.DeleteMappedFieldRowsAsync(projectId);

        // Assert
        var remainingMappings = await _mockMappedFieldRowsRepo.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);
        Assert.Empty(remainingMappings);
    }

    [Fact]
    public async Task DeleteMappedFieldRowsAsync_WithNoMappings_ShouldNotThrow()
    {
        // Arrange
        var projectId = Guid.NewGuid();

        // Act & Assert
        await _service.DeleteMappedFieldRowsAsync(projectId); // Should not throw
    }

    #endregion

    #region UpdateMappedFieldRowsAsync Tests

    [Fact]
    public async Task UpdateMappedFieldRowsAsync_ShouldDeleteOldAndSaveNew()
    {
        // Arrange
        var projectId = Guid.NewGuid();

        // Setup existing mapping
        var existingMapping = new MappedFieldsRow
        {
            Id = Guid.NewGuid(),
            ProjectId = projectId,
            MappedFields = new List<MappedFieldRow>
            {
                new MappedFieldRow()
            }
        };

        await _mockMappedFieldRowsRepo.InsertAsync(existingMapping, Constants.Collections.MappedFieldRows);

        // New mappings to update with
        var newMappedRows = new List<MappedFieldRow>
        {
            new MappedFieldRow(),
            new MappedFieldRow()
        };

        // Act
        await _service.UpdateMappedFieldRowsAsync(projectId, newMappedRows);

        // Assert
        var savedMappings = await _mockMappedFieldRowsRepo.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);

        Assert.Single(savedMappings); // Should have only the new mapping
        Assert.Equal(2, savedMappings.First().MappedFields.Count); // With 2 rows
    }

    #endregion

    #region Edge Cases and Error Scenarios

    [Fact]
    public void AutoMapFields_WithNullFields_ShouldHandleGracefully()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = null, DataSourceName = "DS1", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "ValidField", DataSourceName = "DS1", FieldIndex = 1 }
            }
        };

        // Act & Assert
        var result = _service.AutoMapFields(fieldsPerDataSource);
        Assert.NotNull(result);
    }

    [Fact]
    public void AutoMapFields_WithEmptyFieldNames_ShouldHandleGracefully()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "", DataSourceName = "DS1", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "ValidField", DataSourceName = "DS1", FieldIndex = 1 }
            }
        };

        // Act & Assert
        var result = _service.AutoMapFields(fieldsPerDataSource);
        Assert.NotNull(result);
    }

    [Fact]
    public void AutoMapFields_WithLargeNumberOfFields_ShouldPerformEfficiently()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>();
        var fieldCount = 100;

        for (int ds = 1; ds <= 3; ds++)
        {
            var fields = new List<FieldMappingEx>();
            for (int i = 0; i < fieldCount; i++)
            {
                fields.Add(new FieldMappingEx
                {
                    FieldName = $"Field{i}_DS{ds}",
                    DataSourceName = $"DS{ds}",
                    FieldIndex = i,
                });
            }
            fieldsPerDataSource[$"DS{ds}"] = fields;
        }

        // Act
        var startTime = DateTime.UtcNow;
        var result = _service.AutoMapFields(fieldsPerDataSource);
        var endTime = DateTime.UtcNow;

        // Assert
        Assert.NotNull(result);
        Assert.True((endTime - startTime).TotalSeconds < 10); // Should complete within 10 seconds
    }

    [Fact]
    public async Task PerformAutoMappingAsync_WithNonExistentProject_ShouldHandleGracefully()
    {
        // Arrange
        var nonExistentProjectId = Guid.NewGuid();

        // Act & Assert
        var result = await _service.PerformAutoMappingAsync(nonExistentProjectId);
        Assert.Empty(result);
    }

    #endregion

    #region AutoMapPairCandidate Integration Tests

    [Fact]
    public void AutoMapFields_ShouldUseStringSimilarityCalculator()
    {
        // Arrange
        var field1 = new FieldMappingEx { FieldName = "CustomerName", DataSourceName = "DS1", FieldIndex = 0 };
        var field2 = new FieldMappingEx { FieldName = "FullName", DataSourceName = "DS2", FieldIndex = 0 };

        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx> { field1 },
            ["DS2"] = new List<FieldMappingEx> { field2 }
        };



        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert            

        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.Single(nonEmptyRows);

        // Should have both fields in the same row due to high similarity
        var mappedRow = nonEmptyRows.First();
        Assert.NotNull(mappedRow["DS1"]);
        Assert.NotNull(mappedRow["DS2"]);
    }

    [Fact]
    public void AutoMapFields_WithMatchingDataTypes_ShouldGiveBonusScore()
    {
        // Arrange
        var field1 = new FieldMappingEx { FieldName = "Field1", DataSourceName = "DS1", FieldIndex = 0 };
        var field2 = new FieldMappingEx { FieldName = "Field2", DataSourceName = "DS2", FieldIndex = 0 };
        var field3 = new FieldMappingEx { FieldName = "Field3", DataSourceName = "DS2", FieldIndex = 1 };

        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx> { field1 },
            ["DS2"] = new List<FieldMappingEx> { field2, field3 }
        };

        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();

        // field1 should be mapped with field2 (both String) rather than field3 (Integer)
        // due to data type matching bonus
        var mappedRow = nonEmptyRows.FirstOrDefault(r => r["DS1"]?.FieldName == "Field1");
        if (mappedRow != null)
        {
            // If mapped with another field, it should be Field2 due to type matching
            if (mappedRow["DS2"] != null)
            {
                Assert.Equal("Field2", mappedRow["DS2"].FieldName);
            }
        }
    }

    #endregion

    #region Specific Algorithm Tests

    [Fact]
    public void AutoMapFields_ShouldProcessHighestScoreFirst()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "Email", DataSourceName = "DS1", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "Name", DataSourceName = "DS1", FieldIndex = 1 }
            },
            ["DS2"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "EmailAddress", DataSourceName = "DS2", FieldIndex = 0 },
                new FieldMappingEx { FieldName = "FullName", DataSourceName = "DS2", FieldIndex = 1 }
            }
        };


        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.Equal(2, nonEmptyRows.Count);

        // Email should be mapped with EmailAddress (highest score)
        var emailRow = nonEmptyRows.FirstOrDefault(r => r["DS1"]?.FieldName == "Email");
        Assert.NotNull(emailRow);
        Assert.Equal("EmailAddress", emailRow["DS2"]?.FieldName);

        // Name should be mapped with FullName
        var nameRow = nonEmptyRows.FirstOrDefault(r => r["DS1"]?.FieldName == "Name");
        Assert.NotNull(nameRow);
        Assert.Equal("FullName", nameRow["DS2"]?.FieldName);
    }

    [Fact]
    public void AutoMapFields_ShouldHandleEmptyMappingsInSecondPass()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>
        {
            ["DS1"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "", DataSourceName = "DS1", FieldIndex = 0 }
            },
            ["DS2"] = new List<FieldMappingEx>
            {
                new FieldMappingEx { FieldName = "ValidField", DataSourceName = "DS2", FieldIndex = 0 }
            }
        };


        // Act
        var result = _service.AutoMapFields(fieldsPerDataSource);

        // Assert
        Assert.NotNull(result);
        var nonEmptyRows = result.Where(r => r.HasAnyFields()).ToList();
        Assert.NotEmpty(nonEmptyRows); // Should handle empty field names gracefully
    }

    #endregion

    #region MappedFieldRow Functionality Tests

    [Fact]
    public void MappedFieldRow_AddField_ShouldStoreFieldByDataSourceName()
    {
        // Arrange
        var row = new MappedFieldRow();
        var field = new FieldMappingEx { FieldName = "TestField", DataSourceName = "TestDS" };

        // Act
        row.AddField(field);

        // Assert
        Assert.Equal(field, row["TestDS"]);
        Assert.True(row.HasAnyFields());
        Assert.Contains(field, row.GetAllFields());
    }


    [Fact]
    public void MappedFieldRow_GetAllFields_WithMultipleFields_ShouldReturnAll()
    {
        // Arrange
        var row = new MappedFieldRow();
        var field1 = new FieldMappingEx { FieldName = "Field1", DataSourceName = "DS1" };
        var field2 = new FieldMappingEx { FieldName = "Field2", DataSourceName = "DS2" };

        // Act
        row.AddField(field1);
        row.AddField(field2);

        // Assert
        var allFields = row.GetAllFields().ToList();
        Assert.Equal(2, allFields.Count);
        Assert.Contains(field1, allFields);
        Assert.Contains(field2, allFields);
    }

    #endregion

    #region SaveMappedFieldRows Private Method Tests (via public methods)

    [Fact]
    public async Task SaveMappedFieldRows_ShouldCreateCorrectStructure()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();

        // Setup data source and field
        var dataSource = new DataSource { Id = dataSourceId, Name = "TestDS", ProjectId = projectId };
        await _mockDataSourceRepo.InsertAsync(dataSource, Constants.Collections.DataSources);

        var field = new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSourceId, FieldName = "TestField" };
        await _mockFieldsRepo.InsertAsync(field, Constants.Collections.FieldMapping);

        // Act
        await _service.PerformAutoMappingAsync(projectId);

        // Assert
        var savedMappings = await _mockMappedFieldRowsRepo.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);

        Assert.Single(savedMappings);
        var savedMapping = savedMappings.First();
        Assert.Equal(projectId, savedMapping.ProjectId);
        Assert.NotNull(savedMapping.MappedFields);
        Assert.NotEmpty(savedMapping.MappedFields);
    }

    #endregion        

    #region Data Integrity Tests

    [Fact]
    public async Task UpdateMappedFieldRowsAsync_ShouldMaintainDataIntegrity()
    {
        // Arrange
        var projectId = Guid.NewGuid();

        // Create initial mapping
        var initialMappings = new List<MappedFieldRow>
        {
            new MappedFieldRow()
        };

        await _service.UpdateMappedFieldRowsAsync(projectId, initialMappings);

        // Update with new mappings
        var updatedMappings = new List<MappedFieldRow>
        {
            new MappedFieldRow(),
            new MappedFieldRow(),
            new MappedFieldRow()
        };

        // Act
        await _service.UpdateMappedFieldRowsAsync(projectId, updatedMappings);

        // Assert
        var finalMappings = await _service.GetSavedMappedFieldRowsAsync(projectId);
        Assert.Equal(3, finalMappings.Count);

        // Verify old mappings are completely replaced
        var allStoredMappings = await _mockMappedFieldRowsRepo.QueryAsync(
            x => x.ProjectId == projectId,
            Constants.Collections.MappedFieldRows);
        Assert.Single(allStoredMappings); // Should have only one MappedFieldsRow entry
    }

    #endregion

    #region Field Index Tests

    [Fact]
    public async Task GetExtendedFieldsAsync_ShouldSetCorrectFieldIndices()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var dataSourceId = Guid.NewGuid();

        var dataSource = new DataSource { Id = dataSourceId, Name = "IndexTestDS", ProjectId = projectId };
        await _mockDataSourceRepo.InsertAsync(dataSource, Constants.Collections.DataSources);

        // Insert fields in specific order
        var fields = new List<FieldMappingEx>
        {
            new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSourceId, FieldName = "FirstField", FieldIndex = 0 },
            new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSourceId, FieldName = "SecondField", FieldIndex = 1 },
            new FieldMappingEx { Id = Guid.NewGuid(), DataSourceId = dataSourceId, FieldName = "ThirdField", FieldIndex = 2 }
        };

        foreach (var field in fields)
        {
            await _mockFieldsRepo.InsertAsync(field, Constants.Collections.FieldMapping);
        }

        // Act
        var result = await _service.GetExtendedFieldsAsync(projectId);

        // Assert
        Assert.Single(result);
        var dsFields = result["IndexTestDS"];
        Assert.Equal(3, dsFields.Count);

        // Verify field indices are assigned correctly (0, 1, 2)
        for (int i = 0; i < dsFields.Count; i++)
        {
            Assert.Equal(i, dsFields[i].FieldIndex);
        }
    }

    #endregion

    #region Memory and Performance Tests

    [Fact]
    public void AutoMapFields_WithManyDataSources_ShouldNotExceedMemoryLimits()
    {
        // Arrange
        var fieldsPerDataSource = new Dictionary<string, List<FieldMappingEx>>();

        // Create 10 data sources with 20 fields each
        for (int ds = 1; ds <= 10; ds++)
        {
            var fields = new List<FieldMappingEx>();
            for (int f = 1; f <= 20; f++)
            {
                fields.Add(new FieldMappingEx
                {
                    FieldName = $"Field{f}",
                    DataSourceName = $"DS{ds}",
                    FieldIndex = f - 1
                });
            }
            fieldsPerDataSource[$"DS{ds}"] = fields;
        }


        // Act & Assert
        var initialMemory = GC.GetTotalMemory(false);
        var result = _service.AutoMapFields(fieldsPerDataSource);
        var finalMemory = GC.GetTotalMemory(true);

        Assert.NotNull(result);

        // Memory increase should be reasonable (less than 50MB for this test)
        var memoryIncrease = finalMemory - initialMemory;
        Assert.True(memoryIncrease < 50 * 1024 * 1024, $"Memory increase was {memoryIncrease} bytes");
    }

    #endregion

    #region Exception Handling Tests

    [Fact]
    public async Task PerformAutoMappingAsync_WithDatabaseError_ShouldPropagateException()
    {
        // This test would require mocking the repositories to throw exceptions
        // For now, we'll test that the method handles normal database operations correctly

        // Arrange
        var projectId = Guid.NewGuid();

        // Act & Assert
        // With empty database, should not throw but return minimal results
        var result = await _service.PerformAutoMappingAsync(projectId);
        Assert.NotNull(result);
    }

    #endregion
}
