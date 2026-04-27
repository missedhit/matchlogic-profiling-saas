using MatchLogic.Api.Common;
using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.List;

public class ListDataSourceHandler(
    IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository,
    IGenericRepository<Domain.Import.FileImport, Guid> _fileImportRepository,
    IDataStore dataStore,
    ISecureParameterHandler _secureParameterHandler,
    ILogger<ListDataSourceHandler> logger
    ) : IRequestHandler<ListDataSourceRequest, Result<List<ListDataSourceResponse>>>
{
    public async Task<Result<List<ListDataSourceResponse>>> Handle(ListDataSourceRequest request, CancellationToken cancellationToken)
    {
        // Get All Data Sources for the given project
        var dataSources = await _dataSourceRepository.QueryAsync(x => x.ProjectId == request.ProjectId, Constants.Collections.DataSources);
        var responseList = new List<ListDataSourceResponse>();

        var tasks = dataSources.Select(async item =>
        {
            long size = item.Type >= DataSourceType.FTP ? (item.LastImportedFileMetadata?.Size ?? 0): 0;            
            var decryptedParameters = await _secureParameterHandler.DecryptSensitiveParametersAsync(item.ConnectionDetails.Parameters, item.Id);
            if (decryptedParameters.TryGetValue("FileId", out var fileIdStr) && Guid.TryParse(fileIdStr, out var fileId))
            {
                var fileImportMeta = await _fileImportRepository.GetByIdAsync(fileId, Constants.Collections.ImportFile);
                size = fileImportMeta?.FileSize ?? 0;
                logger.LogInformation("File Meta Info: {fileImportMeta}", fileImportMeta);
            }

            long totalNumberOfCoumns = item.ColumnsCount;
            long totalNumberOfRecords = item.RecordCount;

            //TODO : Depreciated ~ Fetch from data store if not present in meta
            #region Depreciated ~ Fetch from data store if not present in meta
            if (item.ColumnsCount == 0 || item.RecordCount == 0)
            {
                var collectionName = DatasetNames.SnapshotRows(item.ActiveSnapshotId.GetValueOrDefault());
                var dataCollection = await dataStore.GetPagedDataAsync(collectionName, 1, 2);

                if (dataCollection.Data.Any())
                {
                    totalNumberOfCoumns = dataCollection.Data.First().Count - 2; // Exclude _id and Meta
                    totalNumberOfRecords = dataCollection.TotalCount;
                }
            }
            #endregion




            return new ListDataSourceResponse(
                    item.Id,
                    item.Name,
                    item.Type.ToString(),
                    size,
                    totalNumberOfRecords,
                    totalNumberOfCoumns,
                    totalNumberOfRecords,// TODO: Add Valid Number of Records
                    0,                        // TODO: Add Invalid Number of Records                         
                    item.ErrorMessages,
                    item.CreatedAt,
                    item.ModifiedAt
                );
        });

        responseList.AddRange(await Task.WhenAll(tasks));

        if (responseList.Count == 0)
        {
            logger.LogError("No data sources found for project ID: {ProjectId}", request.ProjectId);
            return Result<List<ListDataSourceResponse>>.NotFound($"No data sources found for project ID: {request.ProjectId}.");

        }
        return Result<List<ListDataSourceResponse>>.Success(responseList);
    }
}
