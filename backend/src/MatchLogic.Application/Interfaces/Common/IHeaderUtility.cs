using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Common;

public interface IHeaderUtility
{
    Task<List<string>> GetHeadersAsync(DataSource dataSource, bool fetchCleanse);
    Task<List<string>> GetHeadersFromCollectionAsync(string collectionName);
}
