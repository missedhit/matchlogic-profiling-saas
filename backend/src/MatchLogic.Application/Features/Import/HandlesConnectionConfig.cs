using System;

namespace MatchLogic.Application.Features.Import;

[AttributeUsage(AttributeTargets.Class)]
public class HandlesConnectionConfig : Attribute
{
    public Type ConfigType { get; }

    public HandlesConnectionConfig(Type configType)
    {
        ConfigType = configType;
    }
}