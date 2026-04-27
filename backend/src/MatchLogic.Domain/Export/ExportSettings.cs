using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Export;
public class ExportSettings : IEntity
{
    //public Guid Id { get; set; }
    public Guid ProjectId { get; set; }
    public Guid ProjectRunId { get; set; }
}
