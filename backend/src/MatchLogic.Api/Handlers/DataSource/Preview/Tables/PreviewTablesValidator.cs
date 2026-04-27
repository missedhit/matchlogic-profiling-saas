using FluentValidation;
using System;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Api.Handlers.DataSource.Validators;
using MatchLogic.Api.Common;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Tables;

public class PreviewTablesValidator : AbstractValidator<PreviewTablesRequest>
{
    public PreviewTablesValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;
        RuleFor(x => x.Connection)
           .NotNull()
           .NotEmpty()
           .WithName("Connection")
           .WithMessage(ValidationMessages.CannotBeNull("Connection"));

        RuleFor(x => x.Connection)
            .SetValidator(new BaseConnectionInfoValidator(fileImportRepository));

        When(x => x.Connection.Parameters != null && (x.Connection.Parameters.ContainsKey("Server") || x.Connection.Parameters.ContainsKey("Database")), () =>
        {
            RuleFor(x => x.Connection.Parameters)
                .Must(parameters => parameters.TryGetValue("Database", out var db) && !string.IsNullOrWhiteSpace(db))
                .WithMessage(ValidationMessages.Required("Database parameter"));
        });
    }
}
