using MatchLogic.Application.Features.CleansingAndStandardization.DTOs;
using System.Collections.Generic;
using System;

namespace MatchLogic.Api.Handlers.Cleansing;

/// <summary>
/// Base class for all cleansing rule commands
/// </summary>
/// <typeparam name="TResponse">The type of response that will be returned by the handler</typeparam>
public abstract class BaseCleansingRuleCommand<TResponse> : IRequest<Result<TResponse>>
    where TResponse : ICleansingRuleResponse
{
    /// <summary>
    /// ID of the project containing the data source to apply cleaning rules to
    /// </summary>
    public Guid ProjectId { get; set; }

    /// <summary>
    /// ID of the data source to apply cleaning rules to
    /// </summary>
    public Guid DataSourceId { get; set; }

    /// <summary>
    /// List of standard cleaning rules to apply
    /// </summary>
    public List<CleaningRuleDto> StandardRules { get; set; } = new();

    /// <summary>
    /// List of extended cleaning rules to apply (with copy operations)
    /// </summary>
    public List<ExtendedCleaningRuleDto> ExtendedRules { get; set; } = new();

    /// <summary>
    /// List of mapping rules to apply
    /// </summary>
    public List<MappingRuleDto> MappingRules { get; set; } = new();

    public bool isPreview { get; set; }
}
