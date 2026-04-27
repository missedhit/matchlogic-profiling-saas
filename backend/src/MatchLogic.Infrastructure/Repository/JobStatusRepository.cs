using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Auth.Interfaces;
using MatchLogic.Domain.Entities.Common;
using MediatR;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Repository;

[UseStore(StoreType.ProgressMongoDB)]
public class JobStatusRepository : GenericRepository<JobStatus, Guid>, IJobStatusRepository
{
    private const string CollectionName = Constants.Collections.JobStatus;
    public JobStatusRepository(
        Func<StoreType, IDataStore> storeFactory,
        IStoreTypeResolver storeTypeResolver,
        ICurrentUser currentUser)
        : base(storeFactory, storeTypeResolver, currentUser)
    {
    }    

    public async Task<JobStatus?> GetByJobIdAsync(Guid jobId)
    {
        var list = await QueryAsync(x => x.JobId == jobId, CollectionName);
        return list.FirstOrDefault();
    }

    public async Task UpsertAsync(JobStatus status)
    {
        var existing = await GetByJobIdAsync(status.JobId);
        if (existing is null)
        {
            await InsertAsync(status, CollectionName);
        }
        else
        {
            // Preserve document id for LiteDB update semantics
            status.Id = existing.Id;
            await UpdateAsync(status, CollectionName);
        }
    }

    // Optional helpers if you already use them elsewhere:
    public Task<List<JobStatus>> GetAllAsync()
        => base.GetAllAsync(CollectionName);

    public Task DeleteByJobIdAsync(Guid jobId)
        => base.DeleteAllAsync(x => x.JobId == jobId, CollectionName);
}