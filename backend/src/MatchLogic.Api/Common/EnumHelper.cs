using MatchLogic.Application.Extensions;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using System;

namespace MatchLogic.Api.Common;

public static class EnumHelper
{
    public static string ToString(this StepType enumValue)
    {
        return enumValue.ToString().ToLower();
    }

    public static string ToCollectionName(this StepType enumValue,Guid guid)
    {
        return $"{enumValue.ToString().ToLower()}_{GuidCollectionNameConverter.ToValidCollectionName(guid)}";
    }
}
