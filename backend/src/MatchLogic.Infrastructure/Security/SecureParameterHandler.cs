using MatchLogic.Application.Interfaces.Security;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Security
{
    public class SecureParameterHandler : ISecureParameterHandler
    {
        private readonly IEncryptionService _encryptionService;
        private readonly ILogger<SecureParameterHandler> _logger;
       
        public SecureParameterHandler(IEncryptionService encryptionService, ILogger<SecureParameterHandler> logger)
        {
            _encryptionService = encryptionService;
            _logger = logger;
        }
        public async Task<Dictionary<string, string>> EncryptSensitiveParametersAsync(Dictionary<string, string> parameters, Guid dataSourceId)
        {
            if (parameters == null || parameters.Count == 0)
                return parameters;

            if(dataSourceId == Guid.Empty) return parameters;

            var result = new Dictionary<string, string>(parameters);

            foreach (var kvp in parameters)
            {
                if (!string.IsNullOrEmpty(kvp.Value))
                {
                    try
                    {
                        var encryptedValue = await _encryptionService.EncryptAsync(kvp.Value, dataSourceId.ToString());
                        result[kvp.Key] = encryptedValue;
                        _logger.LogDebug("Encrypted sensitive parameter: {ParameterName}", kvp.Key);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to encrypt parameter: {ParameterName}", kvp.Key);
                        throw;
                    }
                }
            }

            return result;
        }

        public async Task<Dictionary<string, string>> DecryptSensitiveParametersAsync(Dictionary<string, string> parameters, Guid dataSourceId)
        {
            if (parameters == null || parameters.Count == 0)
                return parameters;

            if (dataSourceId == Guid.Empty) return parameters;

            var result = new Dictionary<string, string>(parameters);

            foreach (var kvp in parameters)
            {
                if (!string.IsNullOrEmpty(kvp.Value))
                {
                    try
                    {
                        var decryptedValue = await _encryptionService.DecryptAsync(kvp.Value, dataSourceId.ToString());
                        result[kvp.Key] = decryptedValue;
                        _logger.LogDebug("Decrypted sensitive parameter: {ParameterName}", kvp.Key);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to decrypt parameter: {ParameterName}", kvp.Key);
                        throw;
                    }
                }
            }

            return result;
        }

    }
}