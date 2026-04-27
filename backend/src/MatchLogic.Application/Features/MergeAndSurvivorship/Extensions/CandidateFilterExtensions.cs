using MatchLogic.Domain.MergeAndSurvivorship;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MergeAndSurvivorship.Extensions;

public static class CandidateFilterExtensions
{
    /// <summary>
    /// Filters candidates based on selected data sources if filtering is enabled
    /// </summary>
    /// <param name="candidates">All candidates in the group</param>
    /// <param name="rule">The rule being executed</param>
    /// <param name="config">Configuration with feature flag</param>
    /// <returns>Filtered candidates or original list if filtering disabled/not applicable</returns>
    public static List<RecordCandidate> FilterBySelectedDataSources(
        this List<RecordCandidate> candidates,
        MasterRecordRule rule,
        MasterRecordDeterminationConfig config)
    {
        // Feature disabled globally
        if (!config.UseDataSourceFiltering)
        {
            return candidates;
        }

        // No data sources selected for this rule - consider all
        if (rule.SelectedDataSourceIds == null || !rule.SelectedDataSourceIds.Any())
        {
            return candidates;
        }

        // Filter to only candidates from selected data sources
        var filtered = candidates
            .Where(c => rule.SelectedDataSourceIds.Contains(c.DataSourceId))
            .ToList();

        // Fallback: if no candidates match, return original list
        // This prevents losing all candidates if misconfigured
        return filtered.Any() ? filtered : candidates;
    }

    /// <summary>
    /// Checks if data source filtering resulted in a reduction
    /// </summary>
    public static bool WasFiltered(
        this List<RecordCandidate> original,
        List<RecordCandidate> filtered)
    {
        return filtered.Count < original.Count;
    }
}
