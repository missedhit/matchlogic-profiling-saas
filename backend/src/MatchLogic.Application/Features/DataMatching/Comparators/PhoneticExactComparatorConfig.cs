using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;
public class PhoneticExactComparatorConfig : ComparatorConfig
{
    public override bool CanCreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return (args == null || args.Count == 0) ? true : false;
    }

    public override ComparatorConfig CreateFromArgs(Dictionary<ArgsValue, string> args)
    {
        return new PhoneticExactComparatorConfig();
    }
       
}

