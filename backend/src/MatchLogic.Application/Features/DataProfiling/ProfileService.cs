using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling
{
    public class ProfileService : IProfileService
    {
        private readonly IProfileRepository _repository;

        public ProfileService(IProfileRepository repository)
        {
            _repository = repository;
        }

        /// <summary>
        /// Get profile result by ID
        /// </summary>
        public async Task<ProfileResult> GetProfileResultAsync(Guid profileId, string collectionName)
        {
            return await _repository.GetProfileResultAsync(profileId, collectionName);
        }

        /// <summary>
        /// Get row references by document ID
        /// </summary>
        public async Task<List<RowReference>> GetRowReferencesByDocumentIdAsync(Guid documentId, string collectionName)
        {
            return await _repository.GetRowReferencesByDocumentIdAsync(documentId, collectionName);
        }

        /// <summary>
        /// Get characteristic rows
        /// </summary>
        public async Task<List<RowReference>> GetCharacteristicRowsAsync(
            Guid profileId,
            string columnName,
            ProfileCharacteristic characteristic, string collectioName)
        {
            return await _repository.GetRowReferencesByTypeAsync(
                profileId,
                columnName,
                ReferenceType.Characteristic,
                characteristic.ToString(), collectioName);
        }

        /// <summary>
        /// Get pattern rows
        /// </summary>
        public async Task<List<RowReference>> GetPatternRowsAsync(
            Guid profileId,
            string columnName,
            string patternName, string collectionName)
        {
            return await _repository.GetRowReferencesByTypeAsync(
                profileId,
                columnName,
                ReferenceType.Pattern,
                patternName, collectionName);
        }

        /// <summary>
        /// Get value rows
        /// </summary>
        public async Task<List<RowReference>> GetValueRowsAsync(
            Guid profileId,
            string columnName,
            string value, string collecitonName)
        {
            return await _repository.GetRowReferencesByTypeAsync(
                profileId,
                columnName,
                ReferenceType.Value,
                value, collecitonName);
        }

        /// <summary>
        /// Delete profile result
        /// </summary>
        public async Task DeleteProfileResultAsync(Guid profileId, string collectionName)
        {
            await _repository.DeleteProfileResultAsync(profileId, collectionName);
        }
    }
}
