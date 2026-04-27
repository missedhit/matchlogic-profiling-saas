using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataMatching;
public interface IExpectationMaximisation
{
    void Initialize(List<ProbabilisticMatchCriteria> fields, IAsyncEnumerable<IDictionary<string, object>> records);
    void RunEM();

    IEnumerable<(string FieldName, double MProb, double UProb)> GetResults();
    IEnumerable<(string Pattern, int Count, double Posterior)> GetPatterns();
}
