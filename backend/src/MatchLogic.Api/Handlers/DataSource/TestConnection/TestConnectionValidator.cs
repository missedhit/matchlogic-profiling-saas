using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Validators;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;

namespace MatchLogic.Api.Handlers.DataSource.TestConnection;

public class TestConnectionValidator : AbstractValidator<TestConnectionRequest>
{
    public TestConnectionValidator(IGenericRepository<Domain.Import.FileImport, Guid> fileImportRepository)
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
    }
}
