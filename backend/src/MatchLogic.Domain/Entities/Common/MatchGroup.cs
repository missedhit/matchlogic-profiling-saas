using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Entities.Common;
public class MatchGroup
{
    public int GroupId { get; init; }
    public List<IDictionary<string, object>> Records { get; init; }
    public string GroupHash { get; set; }
    public Dictionary<string, object> Metadata { get; set; }
}
