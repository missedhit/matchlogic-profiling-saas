using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing;

/// <summary>
/// Factory interface for creating transformation rules from configuration
/// </summary>
public interface IRuleFactory
{
    /// <summary>
    /// Creates a transformation rule from a cleaning rule
    /// </summary>
    TransformationRule CreateFromCleaningRule(CleaningRule rule);

    /// <summary>
    /// Creates transformation rules from an extended cleaning rule
    /// </summary>
    IEnumerable<TransformationRule> CreateFromExtendedCleaningRule(ExtendedCleaningRule rule);

    /// <summary>
    /// Creates a transformation rule from a mapping rule
    /// </summary>
    TransformationRule CreateFromMappingRule(MappingRule rule);
}

/// <summary>
/// Enhanced rule factory for creating rules with proper input/output analysis
/// </summary>
public interface IEnhancedRuleFactory
{
    /// <summary>
    /// Creates enhanced transformation rule with input/output analysis
    /// </summary>
    EnhancedTransformationRule CreateEnhancedFromCleaningRule(CleaningRule rule);

    /// <summary>
    /// Creates enhanced transformation rules from extended cleaning rule
    /// </summary>
    IEnumerable<EnhancedTransformationRule> CreateEnhancedFromExtendedCleaningRule(ExtendedCleaningRule rule);

    /// <summary>
    /// Creates enhanced transformation rule from mapping rule
    /// </summary>
    EnhancedTransformationRule CreateEnhancedFromMappingRule(MappingRule rule);

}
