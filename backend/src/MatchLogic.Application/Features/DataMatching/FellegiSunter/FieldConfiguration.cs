using MatchLogic.Application.Interfaces.Comparator;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.FellegiSunter;
public class FieldConfiguration
{
    public string FieldName { get; set; }
    public double MProb { get; set; }  // Probability of agreement given match
    public double UProb { get; set; }  // Probability of agreement given non-match
    public IComparator Comparator { get; set; }
}
public class EMResult
{
    public Dictionary<string, (double MProb, double UProb)> FieldProbabilities { get; set; }
    public double Prior { get; set; }
    public int? Iterations { get; set; }
    public double? LogLikelihood { get; set; }
}
