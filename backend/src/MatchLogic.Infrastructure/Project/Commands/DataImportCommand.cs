using MatchLogic.Application.Common;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Application.Interfaces.Storage;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure.Project.Commands;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Project.Commands;

public class DataImportCommand : BaseCommand
{
    private readonly IDataSourceService _dataSourceService;
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _genericRepository;
    private readonly IRecordHasher _recordHasher;
    private readonly IDataStore _dataStore;
    private readonly IColumnFilter _columnFilter;

    private readonly IConnectionBuilder _connectionBuilder;
    private readonly ISecureParameterHandler _secureParameterHandler;
    private readonly IOAuthTokenService _oauthTokenService;
    private readonly ISchemaValidationService _schemaValidation;
    private readonly IGenericRepository<DataSnapshot, Guid> _snapshotRepo;
    private readonly IGenericRepository<FileImport, Guid> _fileImportRepo;
    private readonly RemoteFileConnectorFactory _remoteFileConnectorFactory;
    private readonly IFileSourceResolver _fileSourceResolver;

    public DataImportCommand(
        IDataSourceService dataSourceService,
        IProjectService projectService,
        IJobEventPublisher jobEventPublisher,
        IGenericRepository<Domain.Project.DataSource, Guid> genericRepository,
        IRecordHasher recordHasher,
        ILogger<DataImportCommand> logger, IDataStore dataStore,
        IGenericRepository<ProjectRun, Guid> projectRunRepository,
        IGenericRepository<StepJob, Guid> stepJobRepository,
        IConnectionBuilder connectionBuilder,
        IColumnFilter columnFilter,
        ISecureParameterHandler secureParameterHandler,
        IOAuthTokenService oauthTokenService,
        RemoteFileConnectorFactory remoteFileConnectorFactory,
        IFileSourceResolver fileSourceResolver,
        IGenericRepository<DataSnapshot, Guid> snapshotRepo = null,
        IGenericRepository<FileImport, Guid> fileImportRepo = null,
        ISchemaValidationService schemaValidation = null)
        : base(projectService, jobEventPublisher, projectRunRepository, stepJobRepository, genericRepository, logger)
    {
        _dataSourceService = dataSourceService;
        _genericRepository = genericRepository;
        _recordHasher = recordHasher;
        _dataStore = dataStore;
        _columnFilter = columnFilter;
        _connectionBuilder = connectionBuilder;
        _secureParameterHandler = secureParameterHandler;
        _oauthTokenService = oauthTokenService;
        _remoteFileConnectorFactory = remoteFileConnectorFactory;
        _fileSourceResolver = fileSourceResolver;
        _snapshotRepo = snapshotRepo;
        _fileImportRepo = fileImportRepo;
        _schemaValidation = schemaValidation;
    }

    protected override int NumberOfSteps => 2;

    protected override async Task<StepData> ExecCommandAsync(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        // TODO (M4): Replace with IQuotaService two-phase enforcement (1000-record lifetime cap).
        var dataSourceId = step.DataSourceId.GetValueOrDefault();
        var dataSource = await _genericRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources)
            ?? throw new InvalidOperationException($"DataSource {dataSourceId} not found.");

        // Step 1: Decrypt parameters temporarily for testing the connection
        var originalParams = dataSource.ConnectionDetails.Parameters;
        var decryptedParameters = await _secureParameterHandler.DecryptSensitiveParametersAsync(originalParams, dataSourceId);

        // Resolve OAuth access token for cloud storage providers (Google Drive, Dropbox, OneDrive)
        if (IsOAuthProvider(dataSource.Type) &&
            decryptedParameters.ContainsKey(RemoteFileConnectionConfig.OAuthDataSourceIdKey))
        {
            var oauthDsId = Guid.Parse(decryptedParameters[RemoteFileConnectionConfig.OAuthDataSourceIdKey]);
            var accessToken = await _oauthTokenService.GetValidAccessTokenAsync(oauthDsId);
            decryptedParameters[RemoteFileConnectionConfig.AccessTokenKey] = accessToken;
        }

        // Temporarily set decrypted parameters for testing
        dataSource.ConnectionDetails.Parameters = decryptedParameters;
        var result = await _dataSourceService.TestConnectionAsync(dataSource, CancellationToken.None);

        // Restore original (encrypted) parameters immediately after testing
        dataSource.ConnectionDetails.Parameters = originalParams;
        if (!result.Success)
        {
            throw new Exception(result.Message);
        }

        // Step 2: Handle FileImportId if provided in the step configuration
        Guid? fileImportId = null;
        if (step.Configuration != null &&
            step.Configuration.TryGetValue("fileImportId", out var value))
        {
            if (value is Guid guid)
            {
                fileImportId = guid;
            }
            else if (value is string str && Guid.TryParse(str, out var parsed))
            {
                fileImportId = parsed;
            }
        }

        // Step 3: Fetch the corresponding FileImport record if available
        FileImport? fileImport = null;
        if (fileImportId.HasValue && fileImportId != Guid.Empty)
        {
            fileImport = await _fileImportRepo.GetByIdAsync(fileImportId.Value, Constants.Collections.ImportFile)
                ?? throw new InvalidOperationException("Invalid fileImportId.");

            if (fileImport.ProjectId != context.ProjectId)
                throw new InvalidOperationException("File does not belong to this project.");

            if (fileImport.DataSourceType != dataSource.Type)
                throw new InvalidOperationException("File type does not match datasource type.");
        }

        // Step 4: Prepare the reader arguments, including fileImport if present.
        // For file uploads, resolve the source via IFileSourceResolver — that
        // downloads the S3 object to /tmp (returning a lease that deletes it
        // when disposed). The lease is held until the reader is fully consumed
        // (end of this method).
        IAsyncDisposable fileLease = NoopAsyncDisposable.Instance;
        if (fileImport != null)
        {
            decryptedParameters["FileId"] = fileImport.Id.ToString();
            var lease = await _fileSourceResolver.ResolveAsync(fileImport.Id, cancellationToken);
            decryptedParameters["FilePath"] = lease.LocalPath;
            fileLease = lease;
        }

        await using var _fileLeaseScope = fileLease;

        var reader = _connectionBuilder
            .WithArgs(dataSource.Type, decryptedParameters, dataSource.Configuration)
            .Build();

        // Step 5: Validate schema using the provided schema validation service
        var headers = reader.GetHeaders();

        // For remote source refreshes (no FileImport), the reader may produce
        // slightly different header formatting than the initial import.
        // Update the signature instead of rejecting so the refresh can proceed.
        if (fileImport == null && IsRemoteSource(dataSource.Type))
        {
            dataSource.SchemaSignature = _schemaValidation.ComputeSignature(
                headers, dataSource.SchemaPolicy);
        }
        else
        {
            _schemaValidation.ValidateHeadersAgainstDataSource(dataSource, headers);
        }

        // Step 6: Create new snapshot (always)
        var snapshotId = Guid.NewGuid();
        var rowsCollection = DatasetNames.SnapshotRows(snapshotId);

        var snapshot = new DataSnapshot
        {
            Id = snapshotId,
            ProjectId = context.ProjectId,
            DataSourceId = dataSource.Id,
            CreatedDate = DateTime.UtcNow,
            FileImportId = fileImport?.Id,
            SchemaSignature = dataSource.SchemaSignature!,
            StoragePrefix = DatasetNames.SnapshotRows(snapshotId),
            Watermark = fileImport != null
            ? $"{{\"fileId\":\"{fileImport.Id}\",\"s3Key\":\"{fileImport.S3Key}\"}}"
                : null
        };

        await _snapshotRepo.InsertAsync(snapshot, Constants.Collections.DataSnapshots);

        // TODO (M4): Replace with IQuotaService.CalculateRemainingAsync — caps maxRowsToImport at remaining quota.
        long? maxRowsToImport = null;

        // Step 8: Import data using the correct import module
        IImportModule importModule = _recordHasher == null ?
            new DataImportModule(reader, _dataStore, _logger, dataSource.Configuration.ColumnMappings, _columnFilter, dataSourceId, maxRowsToImport)
            : new OrderedDataImportModule(reader, _dataStore, _logger, _recordHasher, _jobEventPublisher, context, dataSource.Configuration?.ColumnMappings, _columnFilter, dataSourceId, maxRowsToImport);

        var stepData = new StepData
        {
            Id = Guid.NewGuid(),
            StepJobId = step.Id,
            DataSourceId = dataSourceId,
            CollectionName = rowsCollection,
        };

        var importId = await importModule.ImportDataAsync(rowsCollection, CancellationToken.None);

        // Step 8: Re-fetch to get updated RecordCount/ColumnCount from import
        var freshDataSource = await _genericRepository.GetByIdAsync(dataSourceId, Constants.Collections.DataSources);
        var activeSnapShotID = freshDataSource.ActiveSnapshotId.GetValueOrDefault();

        var removeActiveSnapShot = await _snapshotRepo.GetByIdAsync(activeSnapShotID, Constants.Collections.DataSnapshots);

        if (removeActiveSnapShot != null)
        {
            await _dataStore.DeleteCollection(removeActiveSnapShot.StoragePrefix);
            await _snapshotRepo.DeleteAsync(activeSnapShotID, Constants.Collections.DataSnapshots);
        }

        freshDataSource.ActiveSnapshotId = snapshotId;
        freshDataSource.LatestFileImportId = fileImport?.Id;
        freshDataSource.SchemaSignature = dataSource.SchemaSignature;

        if (fileImport != null)
        {
            var encryptedParameters = await _secureParameterHandler.EncryptSensitiveParametersAsync(decryptedParameters, dataSourceId);
            freshDataSource.ConnectionDetails.Parameters = encryptedParameters;
        }

        // Step 9: Store remote file metadata for change detection (remote sources only)
        if (IsRemoteSource(dataSource.Type))
        {
            freshDataSource.LastImportedFileMetadata = await TryGetRemoteFileMetadata(
                dataSource.Type, decryptedParameters, dataSource.Configuration, dataSourceId);
        }

        await _genericRepository.UpdateAsync(freshDataSource, Constants.Collections.DataSources);

        // TODO (M4): IQuotaService.CommitAsync(freshDataSource.RecordCount) — commit reservation to consumed.

        return stepData;
    }

    protected override Task ValidateInputs(ICommandContext context, StepJob step, CancellationToken cancellationToken = default)
    {
        if (!step.DataSourceId.HasValue)
        {
            throw new InvalidOperationException("DataSourceId is required for import step");
        }
        return Task.CompletedTask;
    }

    private static bool IsOAuthProvider(DataSourceType type)
        => type is DataSourceType.GoogleDrive or DataSourceType.Dropbox or DataSourceType.OneDrive;

    private static bool IsRemoteSource(DataSourceType type)
        => type >= DataSourceType.FTP;

    private sealed class NoopAsyncDisposable : IAsyncDisposable
    {
        public static readonly NoopAsyncDisposable Instance = new();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private async Task<StoredFileMetadata?> TryGetRemoteFileMetadata(
        DataSourceType type,
        Dictionary<string, string> decryptedParams,
        DataSourceConfiguration? sourceConfig,
        Guid dataSourceId)
    {
        try
        {
            var config = new RemoteFileConnectionConfig();
            config.CreateFromArgs(type, decryptedParams, sourceConfig);

            var remotePath = config.RemotePath;
            if (string.IsNullOrEmpty(remotePath))
            {
                _logger.LogWarning("No remote path configured for DataSource {DataSourceId}, skipping metadata capture.", dataSourceId);
                return null;
            }

            using var connector = _remoteFileConnectorFactory.Create(type, config, _logger);
            var metadata = await connector.GetFileMetadataAsync(remotePath);

            return new StoredFileMetadata
            {
                LastModified = metadata.LastModified.ToString("O"),
                ETag = metadata.ETag,
                Size = metadata.Size,
                ContentType = metadata.ContentType
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to capture remote file metadata for DataSource {DataSourceId}. Import will proceed without metadata.", dataSourceId);
            return null;
        }
    }
}
