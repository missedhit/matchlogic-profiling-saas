using MatchLogic.Api.Common.Validators;
using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.MappedFieldRow.Get;

public class MappedFieldRowValidator : AbstractValidator<MappedFieldRowRequest>
{
    public MappedFieldRowValidator(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
            RuleFor(x => x.projectId)
                .SetValidator(new ProjectIdValidator(projectRepository));            
        
    }
}
