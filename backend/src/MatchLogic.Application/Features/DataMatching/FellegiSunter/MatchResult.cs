using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.FellegiSunter;
public class MatchResult
{
    public IDictionary<string, object> Record1 { get; set; }
    public IDictionary<string, object> Record2 { get; set; }
    public double MatchProbability { get; set; }
    public string MatchStatus { get; set; }
    public double CompositeWeight { get; set; }
    public IDictionary<string, FieldScore> FieldScores { get; set; }
}

public class FieldScore
{
    public double Similarity { get; set; }
    public double Weight { get; set; }
    public string Level { get; set; }
    public double M { get; set; }
    public double U { get; set; }
    public double ContributionToTotal { get; set; }
}

public static class MatchResultConverter
{
    public static Dictionary<string, object> ToNestedDictionary(MatchResult matchResult)
    {
        var result = new Dictionary<string, object>
        {
            ["MatchProbability"] = matchResult.MatchProbability,
            ["MatchStatus"] = matchResult.MatchStatus,
            ["CompositeWeight"] = matchResult.CompositeWeight,
            ["Record1"] = matchResult.Record1,
            ["Record2"] = matchResult.Record2
        };

        if (matchResult.FieldScores != null)
        {
            var fieldScoresDict = new Dictionary<string, Dictionary<string, object>>();
            foreach (var kvp in matchResult.FieldScores)
            {
                fieldScoresDict[kvp.Key] = new Dictionary<string, object>
                {
                    ["Similarity"] = kvp.Value.Similarity,
                    ["Weight"] = kvp.Value.Weight,
                    ["Level"] = kvp.Value.Level,
                    ["M"] = kvp.Value.M,
                    ["U"] = kvp.Value.U,
                    ["ContributionToTotal"] = kvp.Value.ContributionToTotal
                };
            }
            result["FieldScores"] = fieldScoresDict;
        }

        return result;
    }
}