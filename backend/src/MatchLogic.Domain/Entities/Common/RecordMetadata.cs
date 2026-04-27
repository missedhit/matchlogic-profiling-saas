using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.Entities.Common;
public record RecordMetadata
{
    public long RowNumber { get; init; }
    public string Hash { get; init; }
    public string SourceFile { get; init; }
    public string BlockingKey { get; set; } = string.Empty;
}
