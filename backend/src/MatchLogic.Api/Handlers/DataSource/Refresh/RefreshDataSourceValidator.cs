using FluentValidation;
using FluentValidation.Results;
using MatchLogic.Api.Common.Validators;
using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Refresh;
using MatchLogic.Api.Handlers.DataSource.Validators;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Security;
using MatchLogic.Domain.Import;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Common;

namespace MatchLogic.Api.Handlers.DataSource.Refresh;

public class RefreshDataSourceValidator : AbstractValidator<RefreshDataSourceRequest>
{
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly IGenericRepository<FileImport, Guid> _fileImportRepository;
    private readonly IConnectionBuilder _connectionBuilder;
    private readonly ISecureParameterHandler _secureParameterHandler;
    private readonly ISchemaValidationService _schemaValidationService;
    private readonly IOAuthTokenService _oauthTokenService;

    public RefreshDataSourceValidator(
        IGenericRepository<FileImport, Guid> fileImportRepository,
        IGenericRepository<Domain.Project.Project, Guid> projectRepository,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IConnectionBuilder connectionBuilder,
        ISecureParameterHandler secureParameterHandler,
        ISchemaValidationService schemaValidationService,
        IOAuthTokenService oauthTokenService)
    {
        _dataSourceRepository = dataSourceRepository;
        _fileImportRepository = fileImportRepository;
        _connectionBuilder = connectionBuilder;
        _secureParameterHandler = secureParameterHandler;
        _schemaValidationService = schemaValidationService;
        _oauthTokenService = oauthTokenService;

        RuleLevelCascadeMode = CascadeMode.Stop;
        ClassLevelCascadeMode = CascadeMode.Stop;

        // Existing validations
        RuleFor(x => x.ProjectId)
            .SetValidator(new ProjectIdValidator(projectRepository));

        RuleFor(x => x.DataSourceId)
            .SetValidator(new DataSourceIdValidator(dataSourceRepository));

        // File validation - only for file-based sources
        RuleFor(x => x.FileImportId)
            .SetValidator(new FileBasedDataSourceValidator(fileImportRepository))
            .When(x => x.FileImportId != Guid.Empty);

        // Schema validation — skip for remote source refreshes (FileImportId == Empty)
        // because the reader may produce slightly different header formatting than the
        // initial import. DataImportCommand re-validates and updates the signature.
        RuleFor(x => x)
            .CustomAsync(ValidateSchemaAsync)
            .When(x => x.FileImportId != Guid.Empty);
    }

    private async Task ValidateSchemaAsync(
        RefreshDataSourceRequest request,
        ValidationContext<RefreshDataSourceRequest> context,
        CancellationToken cancellationToken)
    {
        try
        {
            // Get DataSource
            var dataSource = await _dataSourceRepository.GetByIdAsync(
                request.DataSourceId, Constants.Collections.DataSources);

            if (dataSource == null)
                return; // Already validated by DataSourceIdValidator

            // Decrypt parameters
            var decryptedParameters = await _secureParameterHandler.DecryptSensitiveParametersAsync(
                dataSource.ConnectionDetails.Parameters,
                request.DataSourceId);

            // Handle file-based sources
            if (request.FileImportId != Guid.Empty)
            {
                var fileImport = await _fileImportRepository.GetByIdAsync(
                    request.FileImportId, Constants.Collections.ImportFile);

                if (fileImport == null)
                    return; // Already validated by FileBasedDataSourceValidator

                // Validate file type matches datasource type
                if (fileImport.DataSourceType != dataSource.Type)
                {
                    context.AddFailure(new ValidationFailure("FileImportId",
                        $"File type mismatch. Expected: {dataSource.Type}, Got: {fileImport.DataSourceType}")
                    {
                        ErrorCode = ErrorCodeConstants.SchemaMismatch
                    });
                    return;
                }

                // Add file details to parameters
                decryptedParameters["FileId"] = fileImport.Id.ToString();
                decryptedParameters["FilePath"] = fileImport.FilePath;
            }

            // Resolve OAuth access token for cloud storage providers (Google Drive, Dropbox, OneDrive)
            if (IsOAuthProvider(dataSource.Type) &&
                decryptedParameters.ContainsKey(RemoteFileConnectionConfig.OAuthDataSourceIdKey))
            {
                var oauthDsId = Guid.Parse(decryptedParameters[RemoteFileConnectionConfig.OAuthDataSourceIdKey]);
                var accessToken = await _oauthTokenService.GetValidAccessTokenAsync(oauthDsId, cancellationToken);
                decryptedParameters[RemoteFileConnectionConfig.AccessTokenKey] = accessToken;
            }

            // Build reader and get headers (works for both file and DB sources)
            var reader = _connectionBuilder
                .WithArgs(dataSource.Type, decryptedParameters, dataSource.Configuration)
                .Build();

            var headers = reader.GetHeaders();

            if (headers == null || !headers.Any())
            {
                var fieldName = request.FileImportId != Guid.Empty ? "FileImportId" : "DataSourceId";
                var message = request.FileImportId != Guid.Empty
                    ? "No headers found in uploaded file."
                    : "No columns found in database source.";

                context.AddFailure(new ValidationFailure(fieldName, message)
                {
                    ErrorCode = ErrorCodeConstants.SchemaMismatch
                });
                return;
            }

            // Validate schema using existing service
            _schemaValidationService.ValidateHeadersAgainstDataSource(dataSource, headers);
        }
        catch (InvalidOperationException ex)
        {
            // Schema validation throws InvalidOperationException with descriptive message
            context.AddFailure(new ValidationFailure("Schema", ex.Message)
            {
                ErrorCode = ErrorCodeConstants.SchemaMismatch
            });
        }
        catch (Exception)
        {
            context.AddFailure(new ValidationFailure("Schema",
                "An error occurred while validating schema. Please try again.")
            {
                ErrorCode = ErrorCodeConstants.SchemaMismatch
            });
        }
    }

    private static bool IsOAuthProvider(DataSourceType type)
        => type is DataSourceType.GoogleDrive or DataSourceType.Dropbox or DataSourceType.OneDrive;
}