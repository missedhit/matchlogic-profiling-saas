using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Api.Handlers.DataSource.CheckRemoteUpdates;

public class CheckRemoteUpdatesHandler(
    IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
    ISecureParameterHandler secureParameterHandler,
    IOAuthTokenService oauthTokenService,
    RemoteFileConnectorFactory connectorFactory,
    ILogger<CheckRemoteUpdatesHandler> logger
) : IRequestHandler<CheckRemoteUpdatesRequest, Result<List<CheckRemoteUpdateResult>>>
{
    public async Task<Result<List<CheckRemoteUpdateResult>>> Handle(
        CheckRemoteUpdatesRequest request, CancellationToken cancellationToken)
    {
        // Get all data sources for the project
        var dataSources = await dataSourceRepository.QueryAsync(
            x => x.ProjectId == request.ProjectId,
            Constants.Collections.DataSources);

        // Filter to remote types (FTP and above)
        var remoteSources = dataSources
            .Where(ds => ds.Type >= DataSourceType.FTP)
            .ToList();

        if (remoteSources.Count == 0)
        {
            return Result<List<CheckRemoteUpdateResult>>.Success(new List<CheckRemoteUpdateResult>());
        }

        var results = new List<CheckRemoteUpdateResult>();

        foreach (var ds in remoteSources)
        {
            try
            {
                var result = await CheckSingleDataSource(ds, cancellationToken);
                results.Add(result);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error checking remote updates for DataSource {DataSourceId}", ds.Id);
                results.Add(new CheckRemoteUpdateResult(
                    ds.Id,
                    ds.Name,
                    DataSourceType: (int)ds.Type,
                    HasUpdates: false,
                    CurrentMetadata: null,
                    StoredMetadata: ds.LastImportedFileMetadata,
                    Error: ex.Message
                ));
            }
        }

        return Result<List<CheckRemoteUpdateResult>>.Success(results);
    }

    private async Task<CheckRemoteUpdateResult> CheckSingleDataSource(
        Domain.Project.DataSource ds, CancellationToken ct)
    {
        // Decrypt parameters
        var originalParams = ds.ConnectionDetails.Parameters;
        var decryptedParams = await secureParameterHandler.DecryptSensitiveParametersAsync(
            originalParams, ds.Id);

        try
        {
            // Resolve OAuth access token for cloud storage providers
            if (IsOAuthProvider(ds.Type) &&
                decryptedParams.ContainsKey(RemoteFileConnectionConfig.OAuthDataSourceIdKey))
            {
                var oauthDsId = Guid.Parse(decryptedParams[RemoteFileConnectionConfig.OAuthDataSourceIdKey]);
                var accessToken = await oauthTokenService.GetValidAccessTokenAsync(oauthDsId, ct);
                decryptedParams[RemoteFileConnectionConfig.AccessTokenKey] = accessToken;
            }

            // Create connector config
            var config = new RemoteFileConnectionConfig();
            config.CreateFromArgs(ds.Type, decryptedParams, ds.Configuration);

            // Get the remote path
            var remotePath = config.RemotePath;
            if (string.IsNullOrEmpty(remotePath))
            {
                return new CheckRemoteUpdateResult(
                    ds.Id, ds.Name,
                    DataSourceType: (int)ds.Type,
                    HasUpdates: false,
                    CurrentMetadata: null,
                    StoredMetadata: ds.LastImportedFileMetadata,
                    Error: "No remote path configured for this data source."
                );
            }

            // Get current file metadata from remote
            using var connector = connectorFactory.Create(ds.Type, config, logger);
            var currentMetadata = await connector.GetFileMetadataAsync(remotePath, ct);

            // Compare with stored metadata
            var storedMetadata = ds.LastImportedFileMetadata;
            bool hasUpdates = HasFileChanged(currentMetadata, storedMetadata);

            return new CheckRemoteUpdateResult(
                ds.Id,
                ds.Name,
                DataSourceType: (int)ds.Type,
                HasUpdates: hasUpdates,
                CurrentMetadata: currentMetadata,
                StoredMetadata: storedMetadata,
                Error: null
            );
        }
        finally
        {
            // Restore original (encrypted) parameters — ds object is not saved,
            // but this ensures no accidental leaking of decrypted params.
            ds.ConnectionDetails.Parameters = originalParams;
        }
    }

    private static bool HasFileChanged(RemoteFileMetadata current, StoredFileMetadata? stored)
    {
        if (stored == null)
        {
            // No previous metadata stored — treat as "has updates" (first check)
            return true;
        }

        // Compare last modified
        if (!string.IsNullOrEmpty(stored.LastModified))
        {
            var currentLastModified = current.LastModified.ToString("O");
            if (currentLastModified != stored.LastModified)
                return true;
        }

        // Compare ETag if available
        if (!string.IsNullOrEmpty(stored.ETag) && !string.IsNullOrEmpty(current.ETag))
        {
            if (current.ETag != stored.ETag)
                return true;
        }

        // Compare size
        if (current.Size != stored.Size)
            return true;

        return false;
    }

    private static bool IsOAuthProvider(DataSourceType type)
        => type is DataSourceType.GoogleDrive or DataSourceType.Dropbox or DataSourceType.OneDrive;
}
