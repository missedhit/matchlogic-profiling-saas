using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Regex
{
    public class RegexInfo : AuditableEntity
    {        
        public string Name { get; set; }
        public string Description { get; set; }
        public string RegexExpression { get; set; }
        public bool IsDefault { get; set; }
        public bool IsSystem { get; set; }
        public bool IsSystemDefault { get; set; }
        public bool IsDeleted { get; set; }
        public int Version { get; set; }
    }
}
