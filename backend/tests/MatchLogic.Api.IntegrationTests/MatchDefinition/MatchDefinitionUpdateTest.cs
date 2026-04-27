using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MatchDefinition.Update;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.MatchConfiguration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.MatchDefinition;
[Collection("MatchDefinition Tests")]
public class MatchDefinitionUpdateTest : BaseApiTest
{
    public MatchDefinitionUpdateTest() : base(MatchDefinitionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task UpdateMatchDefinition_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
            new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
        ]);

        // Create initial match definition
        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingMatchDefinition.Id);

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertUpdateMatchDefinitionSuccess(request, response);
    }

    [Fact]
    public async Task UpdateMatchDefinition_WithMultipleDefinitions_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB", "DataSourceC"]);
        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
            new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
        ]);

        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateUpdateMatchDefinitionCommandWithMultipleDefinitions(project, dataSources, existingMatchDefinition.Id);

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertUpdateMatchDefinitionSuccess(request, response);
        response.Value.MatchDefinition.Definitions.Count.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task UpdateMatchDefinition_WithProbabilisticMatching_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
            new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
        ]);

        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateProbabilisticUpdateMatchDefinitionCommand(project, dataSources, existingMatchDefinition.Id);

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertUpdateMatchDefinitionSuccess(request, response);
        response.Value.MatchSetting.IsProbabilistic.Should().BeTrue();
    }

    [Fact]
    public async Task UpdateMatchDefinition_WithNameChange_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
            new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
        ]);

        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingMatchDefinition.Id);

        // Change the name
        request.MatchDefinition.Name = "Updated Match Definition Name";

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertUpdateMatchDefinitionSuccess(request, response);
        response.Value.MatchDefinition.Name.Should().Be("Updated Match Definition Name");
    }

    [Fact]
    public async Task UpdateMatchDefinition_WithFuzzyTextCriteria_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
            new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
        ]);

        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateFuzzyTextUpdateMatchDefinitionCommand(project, dataSources, existingMatchDefinition.Id);

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertUpdateMatchDefinitionSuccess(request, response);
    }

    #endregion

    #region Negative Test Cases    

    [Fact]
    public async Task UpdateMatchDefinition_EmptyId_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var request = new UpdateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                Id = Guid.Empty, // Empty ID
                ProjectId = project.Id,
                Name = "Test",
                Definitions = new List<MatchDefinitionMappedRowDto>()
            },
            MatchSetting = new MatchSettings { ProjectId = project.Id }
        };

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("MatchDefinition.Id", "Id not must be provided"),
            ("MatchDefinition.Definitions", "At least one definition must be provided")
        ]);
    }

    [Fact]
    public async Task UpdateMatchDefinition_EmptyProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var existingId = Guid.NewGuid();
        var request = new UpdateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                Id = existingId,
                ProjectId = Guid.Empty, // Empty ProjectId
                Name = "Test Match Definition",
                Definitions = new List<MatchDefinitionMappedRowDto>()
            },
            MatchSetting = new MatchSettings { ProjectId = Guid.Empty }
        };

        // Act
        var response = await UpdateMatchDefinitionAsync(Guid.Empty, request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("MatchDefinition.ProjectId", "ProjectId must be provided"),
            ("MatchDefinition.Definitions", "At least one definition must be provided")
        ]);
    }

    [Fact]
    public async Task UpdateMatchDefinition_EmptyName_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var existingId = Guid.NewGuid();
        var request = new UpdateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                Id = existingId,
                ProjectId = project.Id,
                Name = "", // Empty name
                Definitions = new List<MatchDefinitionMappedRowDto>()
            },
            MatchSetting = new MatchSettings { ProjectId = project.Id }
        };

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("MatchDefinition.Name", "Name is required"),
            ("MatchDefinition.Definitions", "At least one definition must be provided")
        ]);
    }

    [Fact]
    public async Task UpdateMatchDefinition_EmptyDefinitions_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var existingId = Guid.NewGuid();
        var request = new UpdateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                Id = existingId,
                ProjectId = project.Id,
                Name = "Test Match Definition",
                Definitions = new List<MatchDefinitionMappedRowDto>() // Empty definitions
            },
            MatchSetting = new MatchSettings { ProjectId = project.Id }
        };

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions", "At least one definition must be provided");
    }

    [Fact]
    public async Task UpdateMatchDefinition_InvalidWeight_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingMatchDefinition.Id);

        // Set invalid weight
        request.MatchDefinition.Definitions[0].Criteria[0].Weight = 1.5; // Invalid weight > 1

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions[0].Criteria[0].Weight", "Weight must be between 0 and 1");
    }

    [Fact]
    public async Task UpdateMatchDefinition_DuplicateFieldsInDefinition_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateUpdateMatchDefinitionWithDuplicateFields(project, dataSources, existingMatchDefinition.Id);

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions[0]", "Each field can only be used once within the same definition");
    }

    [Fact]
    public async Task UpdateMatchDefinition_ProbabilisticWithoutRequiredCriteria_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var existingMatchDefinition = await CreateTestMatchDefinitionAsync(project, dataSources);
        var request = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingMatchDefinition.Id);

        // Set probabilistic but only exact criteria
        request.MatchSetting.IsProbabilistic = true;
        request.MatchDefinition.Definitions[0].Criteria[0].MatchingType = MatchingType.Exact;

        // Act
        var response = await UpdateMatchDefinitionAsync(project.Id, request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions", "Probabilistic matching requires at least one exact match criteria and one fuzzy match criteria");
    }    

    #endregion

    #region Helper Methods

    private UpdateMatchDefinitionCommand CreateValidUpdateMatchDefinitionCommand(Project project, List<DataSource> dataSources, Guid existingId)
    {
        return new UpdateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                Id = existingId,
                ProjectId = project.Id,
                Name = "Updated Match Definition",
                Definitions = new List<MatchDefinitionMappedRowDto>
                {
                    new MatchDefinitionMappedRowDto
                    {
                        Id = Guid.NewGuid(),
                        ProjectRunId = Guid.NewGuid(),
                        Criteria = new List<MatchCriterionMappedRowDto>
                        {
                            new MatchCriterionMappedRowDto
                            {
                                Id = Guid.NewGuid(),
                                MatchingType = MatchingType.Exact,
                                DataType = CriteriaDataType.Text,
                                Weight = 0.8,
                                Arguments = new Dictionary<ArgsValue, string>(),
                                MappedRow = new MappedFieldRowDto
                                {
                                    Include = true,
                                    FieldsByDataSource = new Dictionary<string, FieldDto>
                                    {
                                        [dataSources[0].Name.ToLower()] = new FieldDto
                                        {
                                            Id = Guid.NewGuid(),
                                            Name = "UpdatedName",
                                            DataSourceId = dataSources[0].Id,
                                            DataSourceName = dataSources[0].Name
                                        },
                                        [dataSources[1].Name.ToLower()] = new FieldDto
                                        {
                                            Id = Guid.NewGuid(),
                                            Name = "UpdatedFullName",
                                            DataSourceId = dataSources[1].Id,
                                            DataSourceName = dataSources[1].Name
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            MatchSetting = new MatchSettings
            {
                ProjectId = project.Id,
                IsProbabilistic = false,
                MergeOverlappingGroups = true,
                MaxMatchesPerRecord = 1000
            }
        };
    }

    private UpdateMatchDefinitionCommand CreateUpdateMatchDefinitionCommandWithMultipleDefinitions(Project project, List<DataSource> dataSources, Guid existingId)
    {
        var command = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingId);

        // Add second definition
        command.MatchDefinition.Definitions.Add(new MatchDefinitionMappedRowDto
        {
            Id = Guid.NewGuid(),
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                new MatchCriterionMappedRowDto
                {
                    Id = Guid.NewGuid(),
                    MatchingType = MatchingType.Exact,
                    DataType = CriteriaDataType.Text,
                    Weight = 0.7,
                    Arguments = new Dictionary<ArgsValue, string>(),
                    MappedRow = new MappedFieldRowDto
                    {
                        Include = true,
                        FieldsByDataSource = new Dictionary<string, FieldDto>
                        {
                            [dataSources[0].Name.ToLower()] = new FieldDto
                            {
                                Id = Guid.NewGuid(),
                                Name = "Email",
                                DataSourceId = dataSources[0].Id,
                                DataSourceName = dataSources[0].Name
                            },
                            [dataSources[1].Name.ToLower()] = new FieldDto
                            {
                                Id = Guid.NewGuid(),
                                Name = "EmailAddress",
                                DataSourceId = dataSources[1].Id,
                                DataSourceName = dataSources[1].Name
                            }
                        }
                    }
                }
            }
        });

        return command;
    }

    private UpdateMatchDefinitionCommand CreateProbabilisticUpdateMatchDefinitionCommand(Project project, List<DataSource> dataSources, Guid existingId)
    {
        var command = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingId);
        command.MatchSetting.IsProbabilistic = true;

        // Add fuzzy criteria to meet probabilistic requirements
        command.MatchDefinition.Definitions[0].Criteria.Add(new MatchCriterionMappedRowDto
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Fuzzy,
            DataType = CriteriaDataType.Text,
            Weight = 0.6,
            Arguments = new Dictionary<ArgsValue, string>
            {
                [ArgsValue.FastLevel] = "0.8",
                [ArgsValue.Level] = "0.9"
            },
            MappedRow = new MappedFieldRowDto
            {
                Include = true,
                FieldsByDataSource = new Dictionary<string, FieldDto>
                {
                    [dataSources[0].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = "Address",
                        DataSourceId = dataSources[0].Id,
                        DataSourceName = dataSources[0].Name
                    },
                    [dataSources[1].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = "StreetAddress",
                        DataSourceId = dataSources[1].Id,
                        DataSourceName = dataSources[1].Name
                    }
                }
            }
        });

        return command;
    }

    private UpdateMatchDefinitionCommand CreateFuzzyTextUpdateMatchDefinitionCommand(Project project, List<DataSource> dataSources, Guid existingId)
    {
        var command = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingId);
        var criterion = command.MatchDefinition.Definitions[0].Criteria[0];

        criterion.MatchingType = MatchingType.Fuzzy;
        criterion.DataType = CriteriaDataType.Text;
        criterion.Arguments = new Dictionary<ArgsValue, string>
        {
            [ArgsValue.FastLevel] = "0.8",
            [ArgsValue.Level] = "0.9"
        };

        return command;
    }

    private UpdateMatchDefinitionCommand CreateUpdateMatchDefinitionWithDuplicateFields(Project project, List<DataSource> dataSources, Guid existingId)
    {
        var command = CreateValidUpdateMatchDefinitionCommand(project, dataSources, existingId);

        // Add second criterion with same field (duplicate)
        command.MatchDefinition.Definitions[0].Criteria.Add(new MatchCriterionMappedRowDto
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Exact,
            DataType = CriteriaDataType.Text,
            Weight = 0.7,
            Arguments = null,
            MappedRow = new MappedFieldRowDto
            {
                Include = true,
                FieldsByDataSource = new Dictionary<string, FieldDto>
                {
                    // Same field as first criterion - should cause validation error
                    [dataSources[0].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = "UpdatedName", // Duplicate field name
                        DataSourceId = dataSources[0].Id,
                        DataSourceName = dataSources[0].Name
                    },
                    [dataSources[1].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = "Email",
                        DataSourceId = dataSources[1].Id,
                        DataSourceName = dataSources[1].Name
                    }
                }
            }
        });

        return command;
    }

    private async Task<MatchingDataSourcePairs> CreateTestMatchConfigurationAsync(Project project, List<MatchingDataSourcePair> pairs)
    {
        return await new MatchConfigurationBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .WithPairs(pairs)
            .BuildAsync();
    }

    private async Task<MatchDefinitionCollectionMappedRowDto> CreateTestMatchDefinitionAsync(Project project, List<DataSource> dataSources)
    {
        return await new MatchDefinitionBuilder(GetServiceProvider())
            .WithProject(project)
            .WithDataSources(dataSources)
            .WithSingleDefinition()
            .BuildAsync();
    }

    private void AssertUpdateMatchDefinitionSuccess(UpdateMatchDefinitionCommand request, Result<UpdateMatchDefinitionResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MatchDefinition.Should().NotBeNull();
        response.Value.MatchSetting.Should().NotBeNull();
        response.Value.MatchDefinition.ProjectId.Should().Be(request.MatchDefinition.ProjectId);
        response.Value.MatchSetting.ProjectId.Should().Be(request.MatchSetting.ProjectId);
    }

    private async Task<Result<UpdateMatchDefinitionResponse>> UpdateMatchDefinitionAsync(Guid projectId, UpdateMatchDefinitionCommand request)
    {
        var url = $"{RequestURIPath}/{projectId}";
        return await httpClient.PutAndDeserializeAsync<Result<UpdateMatchDefinitionResponse>>(url, StringContentHelpers.FromModelAsJson(request));
    }

    private async Task<Project> CreateTestProjectAsync()
    {
        return await new ProjectBuilder(GetServiceProvider()).WithValid().BuildAsync();
    }

    private async Task<List<DataSource>> CreateTestDataSourceAsync(Project project, string[] names)
    {
        var dataSource = await new DataSourceBuilder(GetServiceProvider())
            .WithProject(project)
            .AddMultipleDataSourcesAsync(names);
        return dataSource.DataSources;
    }

    #endregion
}
