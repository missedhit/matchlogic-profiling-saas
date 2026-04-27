using System.IO;
using System;
using MatchLogic.Domain.Project;
using MatchLogic.Domain.Import;

namespace MatchLogic.Api.Handlers.DataSource.Base;

public abstract class BaseConnectionInfoHandler
{

    private protected BaseConnectionInfo ConfigureConnectionInfo(BaseConnectionInfo baseConnectionInfo)
    {
        if(baseConnectionInfo.Parameters.TryGetValue("FileId",out var fileId) && !string.IsNullOrEmpty(fileId))
        {
            //TODO : We are supporting Multiple extension for any given DataSourceType,so we should get the file path from FileImportRepository instead of hardcoding it. 
            var uploadFolder = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData)
            , "MatchLogicApi"
                , "Uploads");
            var fileExtension = GetExtensionFromSource(baseConnectionInfo.Type);
            baseConnectionInfo.Parameters["FilePath"] = $"{uploadFolder}\\{fileId}{fileExtension}";
            return baseConnectionInfo;
        }

        return baseConnectionInfo;
    }

    private protected string GetExtensionFromSource(DataSourceType type)
    {
        return type switch
        {
            DataSourceType.Excel => ".xlsx",
            DataSourceType.CSV => ".csv",
            _ => throw new NotSupportedException($"Data source type {type} is not supported."),
        };
    }
}
