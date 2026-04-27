using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.DataProfiling;

/// <summary>
/// Document containing row references for a specific characteristic, pattern, or value
/// </summary>
public class RowReferenceDocument : IEntity
{        
    public Guid ProfileResultId { get; set; }
    public string ColumnName { get; set; }

    // Type of row reference (Characteristic, Pattern, Value)
    public ReferenceType Type { get; set; }

    // Key (characteristic name, pattern name, or value)
    public string Key { get; set; }

    public string LookupKey => $"{ProfileResultId}_{ColumnName}_{Type}_{Key}";

    // The actual row references
    public List<RowReference> Rows { get; set; } = new();
}

public enum ReferenceType
{
    Characteristic,
    Pattern,
    Value
}
