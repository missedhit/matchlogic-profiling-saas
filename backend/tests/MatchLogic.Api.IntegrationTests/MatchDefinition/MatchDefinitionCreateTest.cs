using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.MatchDefinition.Create;
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
public class MatchDefinitionCreateTest : BaseApiTest
{
    public MatchDefinitionCreateTest() : base(MatchDefinitionEndpoints.PATH) { }

    #region Positive Test Cases

    [Fact]
    public async Task CreateMatchDefinition_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var request = CreateValidMatchDefinitionCommand(project, dataSources);

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertMatchDefinitionSuccess(request, response);
    }

    [Fact]
    public async Task CreateMatchDefinition_WithMultipleDefinitions_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB", "DataSourceC"]);
        var dataSourcePair = await CreateTestMatchConfigurationAsync(project, [
               new MatchingDataSourcePair(dataSources[0].Id, dataSources[1].Id)
           ]);
        var request = CreateMatchDefinitionCommandWithMultipleDefinitions(project, dataSources);

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertMatchDefinitionSuccess(request, response);
        response.Value.MatchDefinition.Definitions.Count.Should().BeGreaterThan(1);
    }

    [Fact]
    public async Task CreateMatchDefinition_WithProbabilisticMatching_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var request = CreateProbabilisticMatchDefinitionCommand(project, dataSources);

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertMatchDefinitionSuccess(request, response);
        response.Value.MatchSetting.IsProbabilistic.Should().BeTrue();
    }

    [Fact]
    public async Task CreateMatchDefinition_WithFuzzyTextCriteria_ShouldReturn_Success()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var request = CreateFuzzyTextMatchDefinitionCommand(project, dataSources);

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertMatchDefinitionSuccess(request, response);
    }

    #endregion

    #region Negative Test Cases

    [Fact]
    public async Task CreateMatchDefinition_EmptyProjectId_ShouldReturn_ValidationError()
    {
        // Arrange
        var request = new CreateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                ProjectId = Guid.Empty,
                Name = "Test Match Definition",
                Definitions = new List<MatchDefinitionMappedRowDto>()
            },
            MatchSetting = new MatchSettings { ProjectId = Guid.Empty }
        };

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("MatchDefinition.ProjectId", "ProjectId must be provided"),
            ("MatchDefinition.Definitions", "At least one definition must be provided")
        ]);
    }

    [Fact]
    public async Task CreateMatchDefinition_EmptyName_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var request = new CreateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                ProjectId = project.Id,
                Name = "", // Empty name
                Definitions = new List<MatchDefinitionMappedRowDto>()
            },
            MatchSetting = new MatchSettings { ProjectId = project.Id }
        };

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertBaseValidationErrors(response, [
            ("MatchDefinition.Name", "Name is required"),
            ("MatchDefinition.Definitions", "At least one definition must be provided")
        ]);
    }

    [Fact]
    public async Task CreateMatchDefinition_EmptyDefinitions_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var request = new CreateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                ProjectId = project.Id,
                Name = "Test Match Definition",
                Definitions = new List<MatchDefinitionMappedRowDto>() // Empty definitions
            },
            MatchSetting = new MatchSettings { ProjectId = project.Id }
        };

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions", "At least one definition must be provided");
    }

    [Fact]
    public async Task CreateMatchDefinition_InvalidWeight_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var request = CreateValidMatchDefinitionCommand(project, dataSources);

        // Set invalid weight
        request.MatchDefinition.Definitions[0].Criteria[0].Weight = 1.5; // Invalid weight > 1

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions[0].Criteria[0].Weight", "Weight must be between 0 and 1");
    }

    [Fact]
    public async Task CreateMatchDefinition_DuplicateFieldsInDefinition_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var request = CreateMatchDefinitionWithDuplicateFields(project, dataSources);

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions[0]", "Each field can only be used once within the same definition");
    }

    [Fact]
    public async Task CreateMatchDefinition_ProbabilisticWithoutRequiredCriteria_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var request = CreateValidMatchDefinitionCommand(project, dataSources);

        // Set probabilistic but only exact criteria
        request.MatchSetting.IsProbabilistic = true;
        request.MatchDefinition.Definitions[0].Criteria[0].MatchingType = MatchingType.Exact;

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert
        AssertBaseSingleValidationError(response, "MatchDefinition.Definitions", "Probabilistic matching requires at least one exact match criteria and one fuzzy match criteria");
    }

    [Fact]
    public async Task CreateMatchDefinition_FuzzyTextWithoutArguments_ShouldReturn_ValidationError()
    {
        // Arrange
        var project = await CreateTestProjectAsync();
        var dataSources = await CreateTestDataSourceAsync(project, ["DataSourceA", "DataSourceB"]);
        var request = CreateValidMatchDefinitionCommand(project, dataSources);

        // Set fuzzy text without required arguments
        var criterion = request.MatchDefinition.Definitions[0].Criteria[0];
        criterion.MatchingType = MatchingType.Fuzzy;
        criterion.DataType = CriteriaDataType.Text;
        criterion.Arguments = null; // Missing arguments

        // Act
        var response = await CreateMatchDefinitionAsync(request);

        // Assert      
        AssertBaseValidationErrors(response, [
            ("MatchDefinition.Definitions[0].Criteria[0].Arguments", "For Fuzzy Text matching, Fast Level and Level arguments are required"),
            ("MatchDefinition.Definitions[0].Criteria[0].Arguments", "Fast Level and Level must be between 0 and 1")
        ]);
    }

    #endregion

    #region Helper Methods

    private CreateMatchDefinitionCommand CreateValidMatchDefinitionCommand(Project project, List<DataSource> dataSources)
    {
        return new CreateMatchDefinitionCommand
        {
            MatchDefinition = new MatchDefinitionCollectionMappedRowDto
            {
                ProjectId = project.Id,
                Name = "Test Match Definition",
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
                                            Name = "Name",
                                            DataSourceId = dataSources[0].Id,
                                            DataSourceName = dataSources[0].Name
                                        },
                                        [dataSources[1].Name.ToLower()] = new FieldDto
                                        {
                                            Id = Guid.NewGuid(),
                                            Name = "FullName",
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
                MaxMatchesPerRecord = 500
            }
        };
    }

    private CreateMatchDefinitionCommand CreateMatchDefinitionCommandWithMultipleDefinitions(Project project, List<DataSource> dataSources)
    {
        var command = CreateValidMatchDefinitionCommand(project, dataSources);

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

    private CreateMatchDefinitionCommand CreateProbabilisticMatchDefinitionCommand(Project project, List<DataSource> dataSources)
    {
        var command = CreateValidMatchDefinitionCommand(project, dataSources);
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

    private CreateMatchDefinitionCommand CreateFuzzyTextMatchDefinitionCommand(Project project, List<DataSource> dataSources)
    {
        var command = CreateValidMatchDefinitionCommand(project, dataSources);
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

    private async Task<MatchingDataSourcePairs> CreateTestMatchConfigurationAsync(Project project, List<MatchingDataSourcePair> pairs)
    {
        return await new MatchConfigurationBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .WithPairs(pairs)
            .BuildAsync();
    }

    private CreateMatchDefinitionCommand CreateMatchDefinitionWithDuplicateFields(Project project, List<DataSource> dataSources)
    {
        var command = CreateValidMatchDefinitionCommand(project, dataSources);

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
                        Name = "Name", // Duplicate field name
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

    private void AssertMatchDefinitionSuccess(CreateMatchDefinitionCommand request, Result<CreateMatchDefinitionResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.MatchDefinition.Should().NotBeNull();
        response.Value.MatchSetting.Should().NotBeNull();
        response.Value.MatchDefinition.ProjectId.Should().Be(request.MatchDefinition.ProjectId);
        response.Value.MatchDefinition.Name.Should().Be(request.MatchDefinition.Name);
        response.Value.MatchSetting.ProjectId.Should().Be(request.MatchSetting.ProjectId);
    }

    private async Task<Result<CreateMatchDefinitionResponse>> CreateMatchDefinitionAsync(CreateMatchDefinitionCommand request)
    {
        return await httpClient.PostAndDeserializeAsync<Result<CreateMatchDefinitionResponse>>(RequestURIPath, StringContentHelpers.FromModelAsJson(request));
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
