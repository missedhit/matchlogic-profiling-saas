using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Core;
public interface ITelemetry
{
    void MatchFound(long count = 1);
    IDisposable MeasureOperation(string operationName);
    void RecordProcessed(long count = 1);
}
