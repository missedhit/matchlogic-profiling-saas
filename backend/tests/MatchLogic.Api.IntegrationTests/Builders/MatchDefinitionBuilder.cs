using MatchLogic.Application.Features.MatchDefinition.DTOs;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Domain.Entities;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Api.IntegrationTests.Builders;
public class MatchDefinitionBuilder(IServiceProvider serviceProvider)
{
    private readonly IServiceProvider _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private Guid _projectId = Guid.NewGuid();
    private List<DataSource> _dataSources = [];
    private string _name = "Test Match Definition";
    private bool _singleDefinition = true;
    private bool _isProbabilistic = false;
    private MatchSettings _matchSettings;

    public MatchDefinitionBuilder WithProject(Project project)
    {
        if (project?.Id == Guid.Empty)
            throw new ArgumentException("Project ID cannot be empty.", nameof(project));
        _projectId = project.Id;
        return this;
    }

    public MatchDefinitionBuilder WithProjectId(Guid projectId)
    {
        if (projectId == Guid.Empty)
            throw new ArgumentException("Project ID cannot be empty.", nameof(projectId));
        _projectId = projectId;
        return this;
    }

    public MatchDefinitionBuilder WithDataSources(List<DataSource> dataSources)
    {
        _dataSources = dataSources ?? throw new ArgumentNullException(nameof(dataSources));
        return this;
    }

    public MatchDefinitionBuilder WithName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Name cannot be null or empty.", nameof(name));
        _name = name;
        return this;
    }

    public MatchDefinitionBuilder WithSingleDefinition()
    {
        _singleDefinition = true;
        return this;
    }

    public MatchDefinitionBuilder WithMultipleDefinitions()
    {
        _singleDefinition = false;
        return this;
    }

    public MatchDefinitionBuilder WithProbabilisticMatching(bool isProbabilistic = true)
    {
        _isProbabilistic = isProbabilistic;
        return this;
    }

    public MatchDefinitionBuilder WithMatchSettings(MatchSettings matchSettings)
    {
        _matchSettings = matchSettings;
        return this;
    }

    public async Task<MatchDefinitionCollectionMappedRowDto> BuildAsync()
    {

        var matchConfigurationService = _serviceProvider.GetRequiredService<IMatchConfigurationService>();

        var matchDefinition = CreateMatchDefinitionCollection();
        matchDefinition.Id = Guid.Empty;
        var matchSettings = _matchSettings ?? CreateDefaultMatchSettings();

        // Save the match definition
        var matchDefinitionId = await matchConfigurationService.SaveMappedRowConfigurationAsync(matchDefinition);
        var matchSettingId = await matchConfigurationService.SaveSettingsAsync(matchSettings);

        // Return the saved match definition with the new ID
        matchDefinition.Id = matchDefinitionId;
        return matchDefinition;
    }

    private MatchDefinitionCollectionMappedRowDto CreateMatchDefinitionCollection()
    {
        var definitions = new List<MatchDefinitionMappedRowDto>();

        if (_singleDefinition)
        {
            definitions.Add(CreateSingleDefinition());
        }
        else
        {
            definitions.AddRange(CreateMultipleDefinitions());
        }

        return new MatchDefinitionCollectionMappedRowDto
        {
            Id = Guid.NewGuid(),
            ProjectId = _projectId,
            JobId = Guid.NewGuid(),
            Name = _name,
            Definitions = definitions
        };
    }

    private MatchDefinitionMappedRowDto CreateSingleDefinition()
    {
        var criteria = new List<MatchCriterionMappedRowDto>();

        // Add exact text criterion
        criteria.Add(CreateExactTextCriterion("Name", "FullName", 0.8));

        // If probabilistic, add fuzzy criterion
        if (_isProbabilistic)
        {
            criteria.Add(CreateFuzzyTextCriterion("Address", "StreetAddress", 0.6));
        }

        return new MatchDefinitionMappedRowDto
        {
            Id = Guid.NewGuid(),
            ProjectRunId = Guid.NewGuid(),
            Criteria = criteria
        };
    }

    private List<MatchDefinitionMappedRowDto> CreateMultipleDefinitions()
    {
        var definitions = new List<MatchDefinitionMappedRowDto>();

        // First definition - Name matching
        definitions.Add(new MatchDefinitionMappedRowDto
        {
            Id = Guid.NewGuid(),
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                CreateExactTextCriterion("Name", "FullName", 0.8)
            }
        });

        // Second definition - Email matching
        definitions.Add(new MatchDefinitionMappedRowDto
        {
            Id = Guid.NewGuid(),
            ProjectRunId = Guid.NewGuid(),
            Criteria = new List<MatchCriterionMappedRowDto>
            {
                CreateExactTextCriterion("Email", "EmailAddress", 0.9)
            }
        });

        // If probabilistic, add fuzzy definition
        if (_isProbabilistic)
        {
            definitions.Add(new MatchDefinitionMappedRowDto
            {
                Id = Guid.NewGuid(),
                ProjectRunId = Guid.NewGuid(),
                Criteria = new List<MatchCriterionMappedRowDto>
                {
                    CreateFuzzyTextCriterion("Address", "StreetAddress", 0.6)
                }
            });
        }

        return definitions;
    }

    private MatchCriterionMappedRowDto CreateExactTextCriterion(string field1Name, string field2Name, double weight)
    {
        return new MatchCriterionMappedRowDto
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Exact,
            DataType = CriteriaDataType.Text,
            Weight = weight,
            Arguments = new Dictionary<ArgsValue, string>(),
            MappedRow = new MappedFieldRowDto
            {
                Include = true,
                FieldsByDataSource = new Dictionary<string, FieldDto>
                {
                    [_dataSources[0].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = field1Name,
                        DataSourceId = _dataSources[0].Id,
                        DataSourceName = _dataSources[0].Name
                    },
                    [_dataSources[1].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = field2Name,
                        DataSourceId = _dataSources[1].Id,
                        DataSourceName = _dataSources[1].Name
                    }
                }
            }
        };
    }

    private MatchCriterionMappedRowDto CreateFuzzyTextCriterion(string field1Name, string field2Name, double weight, string fastLevel = "0.8", string level = "0.9")
    {
        return new MatchCriterionMappedRowDto
        {
            Id = Guid.NewGuid(),
            MatchingType = MatchingType.Fuzzy,
            DataType = CriteriaDataType.Text,
            Weight = weight,
            Arguments = new Dictionary<ArgsValue, string>
            {
                [ArgsValue.FastLevel] = fastLevel,
                [ArgsValue.Level] = level
            },
            MappedRow = new MappedFieldRowDto
            {
                Include = true,
                FieldsByDataSource = new Dictionary<string, FieldDto>
                {
                    [_dataSources[0].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = field1Name,
                        DataSourceId = _dataSources[0].Id,
                        DataSourceName = _dataSources[0].Name
                    },
                    [_dataSources[1].Name.ToLower()] = new FieldDto
                    {
                        Id = Guid.NewGuid(),
                        Name = field2Name,
                        DataSourceId = _dataSources[1].Id,
                        DataSourceName = _dataSources[1].Name
                    }
                }
            }
        };
    }

    private MatchSettings CreateDefaultMatchSettings()
    {
        return new MatchSettings
        {
            ProjectId = _projectId,
            IsProbabilistic = _isProbabilistic,
            MergeOverlappingGroups = false,
            MaxMatchesPerRecord = 500,
            MaxMatchesPerResultGroup = 0,
            AutogenerateReport = false,
            AdvancedOptions = false
        };
    }
}
