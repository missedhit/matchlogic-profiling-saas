using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Features.Project;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using LiteDB;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DomainDataSource = MatchLogic.Domain.Project.DataSource;

namespace MatchLogic.Infrastructure.Project.DataSource;
public class DataSourceService : IDataSourceService
{
    private const int BatchSize = 1000;
    private const int MaxDegreeOfParallelism = 4;
    private readonly ILogger<DomainDataSource> _logger;
    private readonly IColumnFilter _columnFilter;
    private const int MaxPreviewRecords = 100;
    private readonly IConnectionBuilder connectionBuilder;
    public DataSourceService(ILogger<DomainDataSource> logger, IColumnFilter columnFilter, IConnectionBuilder connectionBuilder)
    {
        _logger = logger;
        _columnFilter = columnFilter;
        this.connectionBuilder = connectionBuilder;
    }
    public async Task<DataSourcePreviewResult> PreviewDataSourceAsync(DomainDataSource dataSource, CancellationToken cancellationToken)
    {
        try
        {
            if (dataSource == null)
                throw new ArgumentNullException(nameof(dataSource));

            var (records,rowsCount, duplicateHeaderCount, errorMessages) = await GetPreviewDataAsync(dataSource, cancellationToken);

            return new DataSourcePreviewResult(records, rowsCount, duplicateHeaderCount, errorMessages);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error previewing data source {DataSourceId}", dataSource?.Id);
            throw;
        }
    }
    public async Task<ConnectionTestResult> TestConnectionAsync(DomainDataSource dataSource, CancellationToken cancellationToken)
    {
        var connectionInfo = dataSource.ConnectionDetails;
        if (connectionInfo == null)
            return new ConnectionTestResult(false, "Unsupported connection type");

        var connection = UpdateConnectionInfo(connectionInfo);

        // Neo4j is export-only — no IConnectionReaderStrategy exists.
        // Test connectivity directly using the Neo4j driver.
        if (dataSource.Type == DataSourceType.Neo4j)
            return await TestNeo4jConnectionAsync(connection.Parameters, cancellationToken);

        var reader = connectionBuilder
            .WithArgs(dataSource.Type, connection.Parameters)
            .Build();

        if (!reader.Config.ValidateConnection())
            return new ConnectionTestResult(false, "Invalid connection parameters");

        var success = await reader.TestConnectionAsync(cancellationToken);
        return new ConnectionTestResult(
            success,
            success ? "Connection successful" : "Connection failed");
    }

    private async Task<ConnectionTestResult> TestNeo4jConnectionAsync(
        Dictionary<string, string> parameters, CancellationToken cancellationToken)
    {
        try
        {
            var server = parameters.GetValueOrDefault("Server", "");
            var port = parameters.GetValueOrDefault("Port", "7687");
            var protocol = parameters.GetValueOrDefault("Protocol", "bolt://");
            var username = parameters.GetValueOrDefault("Username", "neo4j");
            var password = parameters.GetValueOrDefault("Password", "");
            var encryption = parameters.GetValueOrDefault("Encryption", "false");
            var trustCert = parameters.GetValueOrDefault("TrustCertificate", "false");

            if (string.IsNullOrWhiteSpace(server))
                return new ConnectionTestResult(false, "Server address is required");
            if (string.IsNullOrWhiteSpace(password))
                return new ConnectionTestResult(false, "Password is required");

            var uri = $"{protocol}{server}:{port}";

            using var driver = Neo4j.Driver.GraphDatabase.Driver(
                uri,
                Neo4j.Driver.AuthTokens.Basic(username, password),
                o =>
                {
                    if (bool.TryParse(encryption, out var encrypt) && encrypt)
                    {
                        o.WithEncryptionLevel(Neo4j.Driver.EncryptionLevel.Encrypted);
                        if (bool.TryParse(trustCert, out var trust) && trust)
                            o.WithTrustManager(Neo4j.Driver.TrustManager.CreateInsecure());
                    }
                    else if (protocol.Contains("+s://"))
                    {
                        o.WithEncryptionLevel(Neo4j.Driver.EncryptionLevel.Encrypted);
                        if (bool.TryParse(trustCert, out var trust) && trust)
                            o.WithTrustManager(Neo4j.Driver.TrustManager.CreateInsecure());
                    }
                    else
                    {
                        o.WithEncryptionLevel(Neo4j.Driver.EncryptionLevel.None);
                    }
                });

            await driver.VerifyConnectivityAsync();
            return new ConnectionTestResult(true, "Connection successful");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Neo4j connection test failed");
            return new ConnectionTestResult(false, $"Connection failed: {ex.Message}");
        }
    }
    public async Task<DataSourceMetadata> GetMetadataAsync(DomainDataSource dataSource, CancellationToken cancellationToken)
    {
        try
        {
            if (dataSource == null)
                throw new ArgumentNullException(nameof(dataSource));

            //var connectionInfo = dataSource.ConnectionDetails;//_connectionFactory(dataSource.Type) as BaseConnectionInfo;
            var connectionInfo = UpdateConnectionInfo(dataSource.ConnectionDetails);//_connectionFactory(dataSource.Type) as BaseConnectionInfo;
            if(connectionInfo == null)
                throw new NotSupportedException("Unsupported connection type");

            var reader = connectionBuilder
                .WithArgs(dataSource.Type, connectionInfo.Parameters, dataSource.Configuration)
                .Build();

            if (!reader.Config.ValidateConnection())
                throw new NotSupportedException("Not a valid Connection");


            var tables = await reader.GetAvailableTables();
            // Get schema for each table
            foreach (var table in tables)
            {
                var tableSchema = await reader.GetTableSchema($"{table.Schema}.{table.Name}");

                table.Columns = tableSchema.Columns;
            }

            return new DataSourceMetadata(
                tables,
                dataSource.Configuration.ColumnMappings);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting metadata for data source {DataSourceId}", dataSource?.Id);
            throw;
        }
    }

    private BaseConnectionInfo UpdateConnectionInfo(BaseConnectionInfo baseConnectionInfo) 
    {
        if (baseConnectionInfo.Parameters.TryGetValue("FileId", out var fileId) && !string.IsNullOrEmpty(fileId))
        {
            //TODO : We are supporting Multiple extension for any given DataSourceType,so we should get the file path from FileImportRepository instead of hardcoding it. 
            var uploadFolder = MatchLogic.Application.Common.StoragePaths.DefaultUploadPath;
            var fileExtension = GetExtensionFromSource(baseConnectionInfo.Type);
            baseConnectionInfo.Parameters["FilePath"] = Path.Combine(uploadFolder, $"{fileId}{fileExtension}");
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
    public async Task<DomainDataSource> UpdateColumnMappingsAsync(
        DomainDataSource dataSource,
        List<ColumnMappingRequest> mappings,
        CancellationToken cancellationToken)
    {
        //try
        //{
        //    var dataSource = await _dataSourceRepository.GetByIdAsync(dataSourceId)
        //        ?? throw new NotFoundException($"DataSource {dataSourceId} not found");

        //    foreach (var mapping in mappings)
        //    {
        //        dataSource.UpdateColumnMapping(
        //            mapping.SourceColumn,
        //            mapping.Include,
        //            mapping.TargetColumn);
        //    }

        //    await _dataSourceRepository.UpdateAsync(dataSource);
        //    return dataSource;
        //}
        //catch (Exception ex)
        //{
        //    _logger.LogError(ex, "Error updating column mappings for data source {DataSourceId}", dataSourceId);
        //    throw;
        //}
        var domainDataSource = new DomainDataSource();
        return domainDataSource;
    }

    /// <summary>
    /// Retrieves a preview of data from the specified data source.
    /// The returned tuple contains:
    /// 1. A list of dictionaries representing the preview data rows.
    /// 2. The total row count in the data source.
    /// 3. The count of duplicate headers in the data source.
    /// 4. Error Messages during reading data.
    /// </summary>   
    private async Task<Tuple<List<IDictionary<string, object>>,long,long, List<string>?>> GetPreviewDataAsync(
        DomainDataSource dataSource,
        CancellationToken cancellationToken)
    {
        var connection = UpdateConnectionInfo(dataSource.ConnectionDetails);
        var reader = connectionBuilder
            .WithArgs(dataSource.Type, connection.Parameters,dataSource.Configuration)
            .Build();
        try
        {
            var rowCount = reader.RowCount;
            var duplicateHeaderCount = reader.DuplicateHeaderCount;

            var result = await reader.ReadPreviewBatchAsync(new DataImportOptions() 
            { 
                PreviewLimit = 100,
                ColumnMappings = dataSource.Configuration.ColumnMappings 
            },
            _columnFilter,
            cancellationToken);

            return new (result.ToList(), rowCount, duplicateHeaderCount,reader.ErrorMessage);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading preview data from source {DataSourceId}", dataSource.Id);
            throw;
        }
    }
}
