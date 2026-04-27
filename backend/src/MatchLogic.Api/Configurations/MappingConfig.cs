using MatchLogic.Api.Handlers.File.List;
using MatchLogic.Api.Handlers.File.Upload;
using MatchLogic.Api.Handlers.MappedFieldRow.Get;
using MatchLogic.Api.Handlers.MatchConfiguration;
using MatchLogic.Api.Handlers.MatchDefinition.Create;
using MatchLogic.Api.Handlers.Project.List;
using MatchLogic.Api.Handlers.RegexInfo;
using MatchLogic.Domain.Dictionary;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.MatchConfiguration;
using MatchLogic.Domain.Project;
using MatchLogic.Domain.Regex;
using Mapster;
using System.Collections.Generic;

namespace MatchLogic.Api.Configurations;

public static class MapsterConfiguration
{
    public static void RegisterMappings(TypeAdapterConfig config)
    {
        config.Default.PreserveReference(true);



        // Project to ProjectListResponse DTO mapping
        config.NewConfig<Project, ProjectListResponse>()
            .Map(dest => dest.Id, src => src.Id)
            .Map(dest => dest.Name, src => src.Name)
            .Map(dest => dest.Description, src => src.Description)
            .Map(dest => dest.CreatedAt, src => src.CreatedAt)
            .Map(dest => dest.ModifiedAt, src => src.ModifiedAt);


        config.NewConfig<FileImport, FileListResponse>()
            .Map(dest => dest.Id, src => src.Id)
            .Map(dest => dest.FileName, src => src.FileName)
            .Map(dest => dest.OriginalName, src => src.OriginalName)
            .Map(dest => dest.DataSourceType, src => src.DataSourceType)
            .Map(dest => dest.FilePath, src => src.FilePath)
            .Map(dest => dest.FileSize, src => src.FileSize)
            .Map(dest => dest.FileExtension, src => src.FileExtension)
            .Map(dest => dest.CreatedDate, src => src.CreatedDate);

        config.NewConfig<FileImport, FileUploadResponse>()
            .Map(dest => dest.Id, src => src.Id)
            .Map(dest => dest.FileName, src => src.FileName)
            .Map(dest => dest.DataSourceType, src => src.DataSourceType)
            .Map(dest => dest.OriginalName, src => src.OriginalName)
            .Map(dest => dest.FilePath, src => src.FilePath)
            .Map(dest => dest.FileSize, src => src.FileSize)
            .Map(dest => dest.FileExtension, src => src.FileExtension)
            .Map(dest => dest.CreatedDate, src => src.CreatedDate);


        config.NewConfig<RegexInfo, RegexInfoDTO>()
            .Map(dest => dest.Id, src => src.Id)
            .Map(dest => dest.Name, src => src.Name)
            .Map(dest => dest.Description, src => src.Description)
            .Map(dest => dest.RegexExpression, src => src.RegexExpression)
            .Map(dest => dest.IsDefault, src => src.IsDefault)
            .Map(dest => dest.IsSystem, src => src.IsSystem)
            .Map(dest => dest.IsSystemDefault, src => src.IsSystemDefault)
            .Map(dest => dest.Version, src => src.Version);

        //Use Constructor Mapping Explicitly Register for Global Configuration
        TypeAdapterConfig<BaseDataSourcePairDTO, MatchingDataSourcePair>
            .NewConfig()
            .MapWith(src => new MatchingDataSourcePair(src.DataSourceA, src.DataSourceB));


    }
}
