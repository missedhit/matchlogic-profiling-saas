using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.DataProfiling
{
    public class RowReference
    {
        // The complete row data for direct access
        public IDictionary<string, object> RowData { get; set; }

        // The value in the specific column
        public string Value { get; set; }

        // Row metadata for tracking
        public long RowNumber { get; set; }
    }
}
