using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Entities.Common;

public class DataSourceColumnNotes : AuditableEntity
{
    public Guid DataSourceId { get; set; }

    // Key: ColumnName, Value: Note
    public Dictionary<string, string> ColumnNotes { get; set; } = new();

}
