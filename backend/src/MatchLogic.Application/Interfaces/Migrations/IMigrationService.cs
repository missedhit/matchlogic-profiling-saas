using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Migrations
{
    public interface IMigrationService
    {
        Task InitializeDatabase();
        Task<bool> NeedsInitialization();
    }
}
