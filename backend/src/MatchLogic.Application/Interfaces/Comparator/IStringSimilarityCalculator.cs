using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Comparator;
public interface IStringSimilarityCalculator
{
    double CalculateSimilarity(string str1, string str2);
}
