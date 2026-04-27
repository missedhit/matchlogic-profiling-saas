using MatchLogic.Api.Common.Validators;
using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;
using MatchLogic.Api.Handlers.MappedFieldRow.AutoMapping;

namespace MatchLogic.Api.Handlers.MappedFieldRow.Get;

public class AutoMappingValidator : AbstractValidator<AutoMappingCommand>
{
    public AutoMappingValidator(
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
    {
            RuleFor(x => x.projectId)
                .SetValidator(new ProjectIdValidator(projectRepository));            
        
    }
}
