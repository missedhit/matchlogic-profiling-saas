using MatchLogic.Domain.Regex;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Regex
{
    public interface IRegexInfoService
    {
        Task<RegexInfo> CreateRegexInfo(string name, string description, string regexExpression, bool isDefault);
        Task DeleteRegexInfo(Guid id);
        Task<List<RegexInfo>> GetAllRegexInfo();
        Task<RegexInfo> GetRegexInfoById(Guid id);
        Task ResetSystemDefaults();
        Task SetDefault(Guid id, bool isDefault);
        Task UpdateRegexInfo(RegexInfo regexInfoInput);
    }
}
