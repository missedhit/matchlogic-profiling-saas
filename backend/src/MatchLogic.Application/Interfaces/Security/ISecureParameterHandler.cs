using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Security
{
    public interface ISecureParameterHandler
    {
        Task<Dictionary<string, string>> EncryptSensitiveParametersAsync(Dictionary<string, string> parameters, Guid dataSourceId);
        Task<Dictionary<string, string>> DecryptSensitiveParametersAsync(Dictionary<string, string> parameters, Guid dataSourceId);
        //bool IsSensitiveParameter(string parameterName);
    }
}