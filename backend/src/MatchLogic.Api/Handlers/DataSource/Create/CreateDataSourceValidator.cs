using MatchLogic.Api.Common;
using MatchLogic.Api.Common.Validators;
using MatchLogic.Api.Handlers.DataSource.Validators;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.Create;

public class CreateDataSourceValidator : AbstractValidator<CreateDataSourceRequest>
{
    private static readonly HashSet<Domain.Import.DataSourceType> DatabaseTypes =
        new HashSet<Domain.Import.DataSourceType>
        {
            Domain.Import.DataSourceType.LiteDB,
            Domain.Import.DataSourceType.SQLServer,
            Domain.Import.DataSourceType.MySQL,
            Domain.Import.DataSourceType.PostgreSQL,
            Domain.Import.DataSourceType.Snowflake,
            Domain.Import.DataSourceType.Neo4j
        };

    public CreateDataSourceValidator(
        IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository,
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;
        ClassLevelCascadeMode = CascadeMode.Stop;

        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));

        RuleFor(x => x.DataSources)
            .NotNull()
            .NotEmpty()
            .WithName("DataSources")
            .WithMessage("At least one Data Source is required. Data Source list should not be empty.")
            .WithErrorCode(ErrorCodeConstants.Required);

        RuleFor(x => x.DataSources)
            .Must(DataSourceNameExists)
            .WithName("DataSources")
            .WithMessage(ValidationMessages.MustBeUniqueInList("DataSource name"))
            .WithErrorCode(ErrorCodeConstants.NotExists);

        RuleFor(x => x)
            .MustAsync(CheckDataSourceNameUniqueness)
            .WithMessage(ValidationMessages.AlreadyExists("DataSource name"))
            .WithName("DataSources")
            .WithErrorCode(ErrorCodeConstants.NotExists);

        RuleFor(x => x.Connection)
            .NotNull()
            .NotEmpty()
            .WithName("Connection")
            .WithMessage(ValidationMessages.CannotBeNull("Connection"));

        RuleFor(x => x.Connection)
            .SetValidator(new BaseConnectionInfoValidator(fileImportRepository));

        // DATABASE VALIDATION RULES
        When(x => DatabaseTypes.Contains(x.Connection.Type), () =>
        {
            RuleFor(x => x.Connection.Parameters)
                .Must(parameters =>
                    parameters != null &&
                    parameters.TryGetValue("Database", out var db) &&
                    !string.IsNullOrWhiteSpace(db))
                .WithMessage(ValidationMessages.Required("Database parameter"));

            When(x => x.Connection.Parameters != null && x.Connection.Parameters.ContainsKey("Port"), () =>
            {
                RuleFor(x => x.Connection.Parameters)
                    .Must(parameters =>
                        parameters.TryGetValue("Port", out var portStr) &&
                        !string.IsNullOrWhiteSpace(portStr))
                    .WithMessage(ValidationMessages.CannotContainEmptyOrWhitespace("Port parameter"));

                RuleFor(x => x.Connection.Parameters)
                    .Must(parameters =>
                        parameters.TryGetValue("Port", out var portStr) &&
                        int.TryParse(portStr, out var port) &&
                        port > 0 && port <= 65535)
                    .WithMessage(ValidationMessages.Invalid("Port parameter"));
            });

            RuleForEach(x => x.DataSources)
                .ChildRules(dataSource =>
                {
                    dataSource.RuleFor(ds => ds.Name)
                        .NotNull()
                        .NotEmpty()
                        .WithMessage("Name is required for each Data Source.");

                    dataSource.RuleFor(ds => ds.Name)
                        .MaximumLength(Constants.FieldLength.NameMaxLength)
                        .WithMessage(ValidationMessages.MaxLength("Name", Constants.FieldLength.NameMaxLength))
                        .WithErrorCode(ErrorCodeConstants.LimitExceeded);

                    dataSource.When(x => !string.IsNullOrWhiteSpace(x.TableName), () =>
                    {
                        dataSource.RuleFor(ds => ds.TableName)
                            .MaximumLength(Constants.FieldLength.NameMaxLength)
                            .WithMessage(ValidationMessages.MaxLength("Table name", Constants.FieldLength.NameMaxLength))
                            .WithErrorCode(ErrorCodeConstants.LimitExceeded);
                    });

                    dataSource.RuleFor(ds => ds)
                        .Must(ds => !string.IsNullOrWhiteSpace(ds.TableName) || !string.IsNullOrWhiteSpace(ds.Query))
                        .WithMessage("Either TableName or Query must be provided.")
                        .WithErrorCode(ErrorCodeConstants.Required);
                });
        });

        // EXCEL RULES
        When(x => x.Connection.Type == Domain.Import.DataSourceType.Excel, () =>
        {
            RuleForEach(x => x.DataSources)
                .ChildRules(dataSource =>
                {
                    dataSource.RuleFor(ds => ds.TableName)
                        .NotNull()
                        .NotEmpty()
                        .WithMessage("Table name is required for each Data Source.");

                    dataSource.When(x => string.IsNullOrWhiteSpace(x.Name), () =>
                    {
                        dataSource.RuleFor(ds => ds.TableName)
                            .MaximumLength(Constants.FieldLength.NameMaxLength)
                            .WithMessage(ValidationMessages.MaxLength("Table name", Constants.FieldLength.NameMaxLength))
                            .WithErrorCode(ErrorCodeConstants.LimitExceeded);
                    });

                    dataSource.RuleFor(ds => ds.Name)
                        .MaximumLength(Constants.FieldLength.NameMaxLength)
                        .WithMessage(ValidationMessages.MaxLength("Name", Constants.FieldLength.NameMaxLength))
                        .WithErrorCode(ErrorCodeConstants.LimitExceeded);
                });
        });

        // CSV RULES
        When(x => x.Connection.Type == Domain.Import.DataSourceType.CSV, () =>
        {
            RuleForEach(x => x.DataSources)
                .ChildRules(dataSource =>
                {
                    dataSource.RuleFor(ds => ds.Name)
                        .MaximumLength(Constants.FieldLength.NameMaxLength)
                        .WithMessage(ValidationMessages.MaxLength("Name", Constants.FieldLength.NameMaxLength))
                        .WithErrorCode(ErrorCodeConstants.LimitExceeded);
                });
        });

        bool DataSourceNameExists(List<DataSourceRequest> list)
        {
            if (list == null || list.Count == 0) return true;

            var names = list.Select(x => x.Name).ToList();
            return names.Distinct().Count() == names.Count;
        }

        async Task<bool> CheckDataSourceNameUniqueness(CreateDataSourceRequest request, CancellationToken cancellationToken)
        {
            var sources = request.DataSources;
            if (sources == null || sources.Count == 0) return true;

            var nameSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var ds in sources)
            {
                var name = !string.IsNullOrWhiteSpace(ds.Name) ? ds.Name : ds.TableName;
                if (!string.IsNullOrWhiteSpace(name))
                    nameSet.Add(name);
            }

            if (nameSet.Count == 0) return true;

            var dataSources = await dataSourceRepository.QueryAsync(
                x => nameSet.Contains(x.Name) && x.ProjectId == request.ProjectId,
                Constants.Collections.DataSources
            );

            return dataSources.Count == 0;
        }
    }
}
