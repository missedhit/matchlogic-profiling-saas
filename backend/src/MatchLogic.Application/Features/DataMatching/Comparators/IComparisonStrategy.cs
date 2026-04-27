using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public interface IComparisonStrategy
{
    double Compare(string input1, string input2, ComparatorConfig config);
}
