using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Comparator;
public interface IComparator
{
    double Compare(string input1, string input2);
}