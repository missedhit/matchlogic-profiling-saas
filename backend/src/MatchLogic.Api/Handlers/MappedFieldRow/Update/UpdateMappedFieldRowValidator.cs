using MatchLogic.Api.Common.Validators;
using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;
using MatchLogic.Api.Handlers.MappedFieldRow.Get;

namespace MatchLogic.Api.Handlers.MappedFieldRow.Update;

public class UpdateMappedFieldRowValidator : AbstractValidator<UpdateMappedFieldRowCommand>
{
    public UpdateMappedFieldRowValidator(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
            RuleFor(x => x.projectId)
                .SetValidator(new ProjectIdValidator(projectRepository));

            RuleFor(x => x.mappedFieldRows)
                .NotEmpty()
                .WithMessage(ValidationMessages.CannotBeEmpty("Mapped field rows"))
                .NotNull()
                .WithMessage(ValidationMessages.CannotBeNull("Mapped field rows"));            
        
    }
}
