using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class PhoneticFuzzyComparatorConfig : ComparatorConfig
{
    public double PhoneticRating { get; private set; }

    public override bool CanCreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return args.ContainsKey(ArgsValue.PhoneticRating);
    }

    public override ComparatorConfig CreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return new PhoneticFuzzyComparatorConfig
        {
            PhoneticRating = args.TryGetValue(ArgsValue.PhoneticRating, out var phoneticRating) ? Convert.ToDouble(phoneticRating) : 0
        };
    }
}

