using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.Persistence
{
    public interface IProfileRepository
    {
        Task<Guid> SaveProfileResultAsync(
            ProfileResult profileResult,
            Dictionary<string, Dictionary<ProfileCharacteristic, List<RowReference>>> characteristicRowsByColumn,
            Dictionary<string, Dictionary<string, List<RowReference>>> patternRowsByColumn,
            Dictionary<string, Dictionary<string, List<RowReference>>> valueRowsByColumn,
            string collectionName);

        Task<Guid> SaveProfileResultAsync(
            AdvancedProfileResult profileResult,
            Dictionary<string, Dictionary<ProfileCharacteristic, List<RowReference>>> characteristicRowsByColumn,
            Dictionary<string, Dictionary<string, List<RowReference>>> patternRowsByColumn,
            Dictionary<string, Dictionary<string, List<RowReference>>> valueRowsByColumn,
            string collectionName);
        Task<ProfileResult> GetProfileResultAsync(Guid profileId, string collectionName);

        Task<AdvancedProfileResult> GetAdvanceProfileResultAsync(Guid profileId, string collectionName);

        Task<List<RowReference>> GetRowReferencesByDocumentIdAsync(Guid documentId, string collectionName);

        Task<List<RowReference>> GetRowReferencesByTypeAsync(
            Guid profileId,
            string columnName,
            ReferenceType type,
            string key, string collectionName);

        Task DeleteProfileResultAsync(Guid profileId, string collectionName);
    }
}
