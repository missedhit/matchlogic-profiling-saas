using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Regex;
using MatchLogic.Domain.Regex;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Regex;

public class RegexInfoService : IRegexInfoService
{
    private readonly IGenericRepository<RegexInfo, Guid> _regexInfoRepository;
    private readonly ILogger<RegexInfoService> _logger;

    public RegexInfoService(IGenericRepository<RegexInfo, Guid> regexInfoRepository
        , ILogger<RegexInfoService> logger)
    {
        _logger = logger;
        _regexInfoRepository = regexInfoRepository;
    }

    public async Task<List<RegexInfo>> GetAllRegexInfo()
    {
        return await _regexInfoRepository.GetAllAsync(Constants.Collections.RegexInfo);
    }

    public async Task<RegexInfo> GetRegexInfoById(Guid id)
    {
        return await _regexInfoRepository.GetByIdAsync(id, Constants.Collections.RegexInfo);
    }

    public async Task<RegexInfo> CreateRegexInfo(string name, string description, string regexExpression, bool isDefault)
    {
        var regexInfo = new RegexInfo()
        {
            Name = name,
            Description = description,
            RegexExpression = regexExpression,
            IsDefault = isDefault,
            IsSystem = false,
            IsSystemDefault = false,
            IsDeleted = false,
            Version = -1
        };

        await _regexInfoRepository.InsertAsync(regexInfo, Constants.Collections.RegexInfo);

        return regexInfo;
    }

    public async Task UpdateRegexInfo(RegexInfo regexInfoInput)
    {
        var regexInfo = await _regexInfoRepository.GetByIdAsync(regexInfoInput.Id, Constants.Collections.RegexInfo);

        if (regexInfo == null)
        {
            throw new Exception("RegexInfo not found");
        }
        else if (regexInfo.IsSystem)
        {
            throw new Exception("Cannot update system regex");
        }

        regexInfo.Name = regexInfoInput.Name;
        regexInfo.Description = regexInfoInput.Description;
        regexInfo.RegexExpression = regexInfoInput.RegexExpression;
        regexInfo.IsDefault = regexInfoInput.IsDefault;

        await _regexInfoRepository.UpdateAsync(regexInfo, Constants.Collections.RegexInfo);
    }

    public async Task DeleteRegexInfo(Guid id)
    {
        var regexInfo = await _regexInfoRepository.GetByIdAsync(id, Constants.Collections.RegexInfo);

        if (regexInfo == null)
        {
            throw new Exception("RegexInfo not found");
        }
        else if (regexInfo.IsSystem)
        {
            throw new Exception("Cannot delete system regex");
        }

        await _regexInfoRepository.DeleteAsync(regexInfo.Id, Constants.Collections.RegexInfo);
    }

    public async Task SetDefault(Guid id, bool isDefault)
    {
        var regexInfo = await _regexInfoRepository.GetByIdAsync(id, Constants.Collections.RegexInfo);

        if (regexInfo == null)
        {
            throw new Exception("RegexInfo not found");
        }

        regexInfo.IsDefault = isDefault;

        await _regexInfoRepository.UpdateAsync(regexInfo, Constants.Collections.RegexInfo);
    }

    public async Task ResetSystemDefaults()
    {
        var regexInfos = await _regexInfoRepository.GetAllAsync(Constants.Collections.RegexInfo);

        foreach (var regexInfo in regexInfos)
        {

            regexInfo.IsDefault = regexInfo.IsSystemDefault;

            await _regexInfoRepository.UpdateAsync(regexInfo, Constants.Collections.RegexInfo);
        }
    }
}
