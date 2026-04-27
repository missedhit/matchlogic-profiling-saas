using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Cleansing
{
    /// <summary>
    /// Interface for dependency resolver that analyzes rule dependencies
    /// </summary>
    public interface IRuleDependencyResolver
    {
        /// <summary>
        /// Detects and resolves implicit dependencies between rules
        /// </summary>
        Dictionary<Guid, List<Guid>> ResolveImplicitDependencies(IEnumerable<TransformationRule> rules);

        /// <summary>
        /// Validates that there are no circular dependencies between rules
        /// </summary>
        bool ValidateDependencies(Dictionary<Guid, List<Guid>> dependencies);

        /// <summary>
        /// Creates an optimized execution plan based on dependencies
        /// </summary>
        List<Guid> CreateExecutionPlan(Dictionary<Guid, List<Guid>> dependencies);
    }
}
