using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Project;

public enum SchemaPolicy : byte
{
    ReorderInsensitive_NameSensitive = 0,
    StrictExactMatch = 1,
    AllowAdditiveColumns = 2
}
