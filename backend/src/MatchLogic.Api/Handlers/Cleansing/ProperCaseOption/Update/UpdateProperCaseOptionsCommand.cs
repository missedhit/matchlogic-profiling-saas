using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;

namespace MatchLogic.Api.Handlers.Cleansing.ProperCaseOption.Update;

public class UpdateProperCaseOptionsCommand : IRequest<Result<UpdateProperCaseOptionsResponse>>
{
    public ProperCaseOptionsDto Options { get; set; }
}

public record UpdateProperCaseOptionsResponse(ProperCaseOptions Options);

public class ProperCaseOptionsDto
{
    public string Delimiters { get; set; } = " .-'";

    /// <summary>
    /// When FALSE: Check exceptions with case-insensitive matching
    /// When TRUE: Skip exception checking entirely
    /// </summary>
    public bool IgnoreCaseOnExceptions { get; set; } = false;

    public List<string> Exceptions { get; set; } = new List<string>();

    public ActionOnException ActionOnException { get; set; } = ActionOnException.ConvertToUpper;

    public DateTime? CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; } = DateTime.UtcNow;

    // Future: public string UserId { get; set; }
}