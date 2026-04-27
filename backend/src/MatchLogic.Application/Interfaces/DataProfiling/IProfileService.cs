using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Interfaces.DataProfiling
{
    public interface IProfileService
    {
        Task<ProfileResult> GetProfileResultAsync(Guid profileId, string collectionName);

        Task<List<RowReference>> GetRowReferencesByDocumentIdAsync(Guid documentId, string collectionName);

        Task<List<RowReference>> GetCharacteristicRowsAsync(
            Guid profileId,
            string columnName,
            ProfileCharacteristic characteristic,
            string collectionName);

        Task<List<RowReference>> GetPatternRowsAsync(
            Guid profileId,
            string columnName,
            string patternName, string collectionName);

        Task<List<RowReference>> GetValueRowsAsync(
            Guid profileId,
            string columnName,
            string value, string collectionName);

        Task DeleteProfileResultAsync(Guid profileId, string collectionName);
    }
}
