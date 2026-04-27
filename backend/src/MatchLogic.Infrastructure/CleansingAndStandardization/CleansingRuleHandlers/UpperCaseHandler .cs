using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Domain.CleansingAndStandaradization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers
{
    [CleansingOperation(CleaningRuleType.UpperCase)]
    public class UpperCaseHandler : ICleansingOperationHandler
    {
        public UpperCaseHandler(ILogger<UpperCaseHandler> logger) { }

        public CleaningRuleType RuleType => CleaningRuleType.UpperCase;
        public bool CanHandle(CleaningRuleType operationType) =>
            operationType == CleaningRuleType.UpperCase;

        public object ProcessValue(object value)
        {
            if (value == null) return null;
            return value?.ToString()?.ToUpperInvariant();
        }
    }
}
