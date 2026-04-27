using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class NumericComparatorConfig : ComparatorConfig
{
    public decimal UpperLimit { get; private set; }
    public decimal LowerLimit { get; private set; }

    public override bool CanCreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return args.ContainsKey(ArgsValue.UpperLimit) && args.ContainsKey(ArgsValue.LowerLimit);
    }

    public override ComparatorConfig CreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return new NumericComparatorConfig
        {
            UpperLimit = args.TryGetValue(ArgsValue.UpperLimit, out var upper) ? Convert.ToDecimal(upper) : 0,
            LowerLimit = args.TryGetValue(ArgsValue.LowerLimit, out var lower) ? Convert.ToDecimal(lower) : 0
        };
    }
}
