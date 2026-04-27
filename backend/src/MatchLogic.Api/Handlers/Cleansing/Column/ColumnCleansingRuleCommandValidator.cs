using MatchLogic.Api.Common;
using MatchLogic.Application.Interfaces.Persistence;
using FluentValidation;
using System;
using System.Threading.Tasks;
using System.Threading;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.Cleansing.Column;

//public class ColumnCleansingRuleCommandValidator
//    : BaseCleansingRuleCommandValidator<ColumnCleansingRuleCommand, ColumnCleansingRuleResponse>
//{
//    public ColumnCleansingRuleCommandValidator(
//        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
//        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository)
//        : base(projectRepository, dataSourceRepository)
//    {
//        // Add any Create-specific validation rules here
//    }
//}
