using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence;
public class SqliteDataStore : IDataStore
{
    private readonly string _connectionString;
    private SqliteConnection _connection;
    private readonly ILogger _logger;
    private const int CommitBatchSize = 10000;

    public SqliteDataStore(string connectionString, ILogger logger)
    {
        _connectionString = connectionString;
    }

    public async Task<Guid> InitializeJobAsync(string collectionName = "")
    {
        try
        {
            if (_connection == null)
            {
                _connection = new SqliteConnection(_connectionString);
                await _connection.OpenAsync();
            }

            var jobId = Guid.NewGuid();
            using var command = _connection.CreateCommand();
            command.CommandText = $@"
                CREATE TABLE IF NOT EXISTS Job_{jobId.ToString("N")} (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Data TEXT NOT NULL
                )";
            await command.ExecuteNonQueryAsync();

            _logger.LogInformation("Initialized new import job: {JobId}", jobId);
            return jobId;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize new import job");
            throw;
        }
    }

    public IAsyncEnumerable<IDictionary<string, object>> StreamDataAsync(string collectionName, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Task<bool> DeleteCollection(string collectionName) => throw new NotImplementedException();

    public async Task InsertBatchAsync(Guid jobId, IEnumerable<IDictionary<string, object>> batch, string collectionName = "")
    {
        try
        {
            var transaction = _connection.BeginTransaction();
            using var command = _connection.CreateCommand();
            command.CommandText = $"INSERT INTO Job_{jobId.ToString("N")} (Data) VALUES (@Data)";
            var paramData = command.CreateParameter();
            paramData.ParameterName = "@Data";
            command.Parameters.Add(paramData);

            int count = 0;
            foreach (var item in batch)
            {
                paramData.Value = JsonSerializer.Serialize(item);
                await command.ExecuteNonQueryAsync();
                count++;

                if (count % CommitBatchSize == 0)
                {
                    await transaction.CommitAsync();
                    transaction.Dispose();
                    transaction = _connection.BeginTransaction();
                    _logger.LogDebug("Committed {Count} records to job {JobId}", CommitBatchSize, jobId);
                }
            }

            await transaction.CommitAsync();
            _logger.LogInformation("Inserted batch of {Count} records into job {JobId}", count, jobId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error inserting batch into job {JobId}", jobId);
            throw;
        }
    }

    public async Task InsertBatchAsync(string collectionName, IEnumerable<IDictionary<string, object>> batch) => throw new NotImplementedException();

    public async Task<IEnumerable<IDictionary<string, object>>> GetJobDataAsync(Guid jobId)
    {
        try
        {
            using var command = _connection.CreateCommand();
            command.CommandText = $"SELECT Data FROM Job_{jobId.ToString("N")}";

            var data = new List<IDictionary<string, object>>();
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var jsonData = reader.GetString(0);
                if (!String.IsNullOrEmpty(jsonData))
                {
                    var jsonObject = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonData);
                    if (jsonObject != null)
                    {
                        data.Add(jsonObject);
                    }
                    else
                    {
                        _logger.LogError("Error deserializing data for job {JobId}", jobId);
                    }
                }

            }

            _logger.LogInformation("Retrieved {Count} records from job {JobId}", data.Count, jobId);
            return data;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving data for job {JobId}", jobId);
            throw;
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobDataAsync(Guid jobId, int pageNumber, int pageSize)
    {
        try
        {
            using var countCommand = _connection.CreateCommand();
            countCommand.CommandText = $"SELECT COUNT(*) FROM Job_{jobId.ToString("N")}";
            var totalCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync());

            using var dataCommand = _connection.CreateCommand();
            dataCommand.CommandText = $@"
                SELECT Data FROM Job_{jobId.ToString("N")} 
                ORDER BY Id 
                LIMIT @PageSize OFFSET @Offset";
            dataCommand.Parameters.AddWithValue("@PageSize", pageSize);
            dataCommand.Parameters.AddWithValue("@Offset", (pageNumber - 1) * pageSize);

            var data = new List<IDictionary<string, object>>();
            using var reader = await dataCommand.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var jsonData = reader.GetString(0);
                var jsonObject = JsonSerializer.Deserialize<Dictionary<string, object>>(jsonData);
                if (jsonObject != null)
                {
                    data.Add(jsonObject);
                }
            }

            _logger.LogInformation("Fetched page {PageNumber} with {Count} records from job {JobId}", pageNumber, data.Count, jobId);
            return (data, totalCount);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error fetching paged data for job {JobId}", jobId);
            throw;
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedDataAsync(string collectionName, int pageNumber, int pageSize) => throw new NotImplementedException();

    public IAsyncEnumerable<IDictionary<string, object>> StreamJobDataAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        _connection?.Dispose();
        _logger.LogInformation("Disposed SQLite connection");
    }

    public Task<T> GetByIdAsync<T, Tkey>(Tkey id, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task<List<T>> GetAllAsync<T>(string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task InsertAsync<T>(T entity, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync<T>(T entity, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task DeleteAsync<Tkey>(Tkey id, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task<List<T>> QueryAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task<int> DeleteAllAsync<T>(Expression<Func<T, bool>> predicate, string collectionName)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<IDictionary<string, object>> StreamJobDataAsync(Guid jobId, IStepProgressTracker progressTracker, string collectionName = "", CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<IDictionary<string, object>> GetRandomSample(double maxPairs, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task SampleAndStoreTempData(string sourceCollectionName, string tempCollectionName, double maxPairs)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<IDictionary<string, object>> GetStreamFromTempCollection(string _tempCollectionName, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task InsertProbabilisticBatchAsync(string collectionName, IEnumerable<MatchResult> batch)
    {
        throw new NotImplementedException();
    }

    public Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedJobWithSortingAndFilteringDataAsync(string collectionName, int pageNumber, int pageSize, string filterText = null, string sortColumn = null, bool ascending = true, string filters = "",
            GroupQueryFilter groupFilter = null)
    {
        throw new NotImplementedException();
    }

    public Task BulkInsertAsync<T>(IEnumerable<T> entity, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync(IDictionary<string, object> entity, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetPagedWithSmartFilteringAndProjectionAsync(string collectionName, int pageNumber, int pageSize, string filterText = null, string sortColumn = null, bool ascending = true, string filters = "")
    {
        throw new NotImplementedException();
    }


    public Task BulkUpdateAsync(IEnumerable<IDictionary<string, object>> entities, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task<bool> UpdateByFieldAsync<TField>(IDictionary<string, object> data, string collectionName, string fieldName, TField fieldValue)
    {
        throw new NotImplementedException();
    }

    public Task<bool> RenameCollection(string oldName, string newName)
    {
        throw new NotImplementedException();
    }

    public Task<bool> CollectionExistsAsync(string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task BulkUpsertAsync<T>(IEnumerable<T> entities, string collectionName)
    {
        throw new NotImplementedException();
    }

    public Task BulkUpsertByFieldsAsync<T>(IEnumerable<T> entities, string collectionName, Expression<Func<T, object>>[] matchFields)
    {
        throw new NotImplementedException();
    }

    public Task CreateGroupFilterIndexesAsync(string collectionName)
    {
        throw new NotImplementedException();
    }
}
