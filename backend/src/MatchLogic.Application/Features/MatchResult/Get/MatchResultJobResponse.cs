using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchResult.Get;
public class MatchResultJobResponse
{
    public Guid JobId { get; set; }

    public Guid Id { get; set; }

    public string Status { get; set; }

    public int ProcessedRecords { get; set; }
    public int TotalRecords { get; set; }
    public string Error { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }

    public Dictionary<string, object> Metadata { get; set; }

    public Dictionary<string, JobStepInfo> Steps { get; set; }

    public string StatusUrl { get; set; }
}