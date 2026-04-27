using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class StringComparatorConfig : ComparatorConfig
{
    public double Level { get; private set; }

    public override bool CanCreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return args.ContainsKey(ArgsValue.Level);
    }

    public override ComparatorConfig CreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return new StringComparatorConfig
        {
            Level = Convert.ToDouble(args[ArgsValue.Level])
        };
    }
}
