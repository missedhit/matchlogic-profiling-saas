using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.DataSource.Preview.Columns;
public class PreviewColumnsValidator : AbstractValidator<PreviewColumnsRequest>
{
    public PreviewColumnsValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository)
    {
        RuleLevelCascadeMode = CascadeMode.Stop;
        ClassLevelCascadeMode = CascadeMode.Stop;
        

        RuleFor(x => x.Connection)
           .NotNull()
           .NotEmpty()
           .WithName("Connection")
           .WithMessage(ValidationMessages.CannotBeNull("Connection"));


        RuleFor(x => x.Connection)
            .SetValidator(new BaseConnectionInfoValidator(fileImportRepository));

        When(x => x.Connection.Parameters != null && (x.Connection.Parameters.ContainsKey("Server") || x.Connection.Parameters.ContainsKey("Database")),() =>
            {
                RuleFor(x => x.Connection.Parameters)
                    .Must(parameters=> parameters.TryGetValue("Database", out var db) && !string.IsNullOrWhiteSpace(db))
                    .WithMessage(ValidationMessages.Required("Database parameter"));
            });
        
    }
}
