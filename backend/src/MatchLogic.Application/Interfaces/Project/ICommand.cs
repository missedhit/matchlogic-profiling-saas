using MatchLogic.Application.Features.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Project
{
    public interface ICommand
    {
        Task ExecuteAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default);
    }

    public interface ICommandFactory
    {
        ICommand GetCommand(StepType stepType);
    }
    public interface ICommandContext
    {
         Guid RunId { get; set; }
        Guid StepId { get; set; }
         Guid ProjectId { get; set; }
         Task InitializeContext();
        List<StepData> GetStepOutput(StepType stepType, Guid? dataSourceId = null);
         void SetStepOutput(StepType stepType, StepData output);
         Dictionary<string, string> GetCollectionNames(StepType stepType);
         string CreateCollectionName(Guid stepId, string baseName);

        FlowStatistics Statistics {  get; }
    }

    public interface IColumnFilter
    {
        IDictionary<string, object> FilterColumns(
            IDictionary<string, object> row,
            Dictionary<string, ColumnMapping> columnMappings);
    }
}
