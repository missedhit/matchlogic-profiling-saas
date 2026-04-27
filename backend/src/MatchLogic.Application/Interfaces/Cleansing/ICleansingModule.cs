using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing;

public interface ICleansingModule
{
    Task<Guid> ProcessDataAsync(
        string inputCollection,
        string outputCollection,
       EnhancedCleaningRules fieldOperations,
       ICommandContext commandContext = null,
       bool isPreview = false,
        CancellationToken cancellationToken = default);

    Task<SchemaInfo> GetOutputSchemaAsync(EnhancedCleaningRules fieldOperations);
}

public interface ICleansingOperationHandler
{
    CleaningRuleType RuleType { get; }
    bool CanHandle(CleaningRuleType operationType);
    object ProcessValue(object value);
}
