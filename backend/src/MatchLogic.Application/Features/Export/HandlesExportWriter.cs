using MatchLogic.Domain.Import;
using System;
namespace MatchLogic.Application.Features.Export;

[AttributeUsage(AttributeTargets.Class)]
public class HandlesExportWriter : Attribute
{
    public DataSourceType DataSourceType { get; }

    public HandlesExportWriter(DataSourceType configType)
    {
        DataSourceType = configType;
    }
}
