using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public abstract class ComparatorConfig
{
    public abstract bool CanCreateFromArgs(Dictionary<ArgsValue, string> args);
    public abstract ComparatorConfig CreateFromArgs(Dictionary<ArgsValue, string> args);
}
