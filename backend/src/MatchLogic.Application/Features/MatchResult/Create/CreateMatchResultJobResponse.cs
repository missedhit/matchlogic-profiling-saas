using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchResult.Create;
public class CreateMatchResultJobResponse 
{
    public Guid JobId { get; set; }

    public Guid Id { get; set; }

    public string Status { get; set; }

    public string JobDetail { get; set; }

    public string StatusUrl { get; set; }
}
