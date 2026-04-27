using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;

public class PercentageNumericComparatorConfig : ComparatorConfig
{
    /// <summary>
    /// Upper bound percentage (e.g., 10 for 10%)
    /// </summary>
    public decimal UpperPercentage { get; private set; }

    /// <summary>
    /// Lower bound percentage (e.g., 10 for 10%)
    /// </summary>
    public decimal LowerPercentage { get; private set; }

    public override bool CanCreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return args.ContainsKey(ArgsValue.UpperPercentage) && args.ContainsKey(ArgsValue.LowerPercentage);
    }

    public override ComparatorConfig CreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return new PercentageNumericComparatorConfig
        {
            UpperPercentage = args.TryGetValue(ArgsValue.UpperPercentage, out var upper) ? Convert.ToDecimal(upper) : 0,
            LowerPercentage = args.TryGetValue(ArgsValue.LowerPercentage, out var lower) ? Convert.ToDecimal(lower) : 0
        };
    }
}