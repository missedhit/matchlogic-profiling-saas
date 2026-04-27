using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Persistence;
public interface IJobStatusRepository : IGenericRepository<JobStatus, Guid>
{
}
