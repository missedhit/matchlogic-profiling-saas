using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching.Comparators;

[AttributeUsage(AttributeTargets.Class)]
public class HandlesConfigAttribute : Attribute
{
    public Type ConfigType { get; }

    public HandlesConfigAttribute(Type configType)
    {
        ConfigType = configType;
    }
}
