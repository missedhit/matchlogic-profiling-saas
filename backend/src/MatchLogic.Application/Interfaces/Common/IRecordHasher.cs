using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Common;
public interface IRecordHasher
{
    string ComputeHash(IDictionary<string, object> record);
    string ComputeGroupHash(List<IDictionary<string, object>> records);

}