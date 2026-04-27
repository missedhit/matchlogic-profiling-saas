using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization;
public class TransformationGraph : IEntity
{  
    public Guid ProjectId { get; set; }
    public Guid DataSourceId { get; set; }

    // Store graph JSON as raw string
    public string GraphJson { get; set; } = string.Empty;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public DateTime? UpdatedAt { get; set; }
}
