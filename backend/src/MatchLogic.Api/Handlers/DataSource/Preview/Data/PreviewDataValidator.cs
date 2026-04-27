using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Data;

public class PreviewDataRequestValidator : AbstractValidator<PreviewDataRequest>
{
    public PreviewDataRequestValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository)
    {

        RuleLevelCascadeMode = CascadeMode.Stop;
        RuleFor(x => x.Connection)
           .NotNull()
           .NotEmpty()
           .WithName("Connection")
           .WithMessage(ValidationMessages.CannotBeNull("Connection"));

        RuleFor(x => x.Connection)
            .SetValidator(new BaseConnectionInfoValidator(fileImportRepository));

        // TableName is required for all types except CSV
        When(x => x.Connection.Type == Domain.Import.DataSourceType.Excel, () =>
        {
            RuleFor(x => x.TableName)
                .NotNull()
                .WithMessage(ValidationMessages.CannotBeNull("TableName"))
                .NotEmpty()
                .WithMessage(ValidationMessages.CannotBeEmpty("TableName"));
        });


        When(x => x.Connection.Parameters != null && (x.Connection.Parameters.ContainsKey("Server") || x.Connection.Parameters.ContainsKey("Database")), () =>
        {
            RuleFor(x => x.Connection.Parameters)
                .Must(parameters => parameters.TryGetValue("Database", out var db) && !string.IsNullOrWhiteSpace(db))
                .WithMessage(ValidationMessages.Required("Database parameter"));
            RuleFor(x => x)
                .Must(x =>!string.IsNullOrWhiteSpace(x.TableName) || !string.IsNullOrWhiteSpace(x.Query))
                .WithMessage("Either TableName or Query parameter must be provided.");
        });

    }

}
