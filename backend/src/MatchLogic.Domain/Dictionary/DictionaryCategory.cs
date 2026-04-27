using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Dictionary;

public class DictionaryCategory : AuditableEntity
{        
    public string Name { get; set; }
    public string Description { get; set; }
    public List<string> Items { get; set; } = new();
    public bool IsSystem { get; set; }
    public bool IsDefault { get; set; }
    public bool IsDeleted { get; set; }
    public int Version { get; set; }
}
