using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataMatching;
public class IndexEntry
{
    //public Dictionary<string, HashSet<uint>> FieldHashes { get; } = new Dictionary<string, HashSet<uint>>();

    public Dictionary<string, HashSet<uint>> FieldHashes { get; set; }
            = new();
    public IDictionary<string, object> Record { get; set; }

}
