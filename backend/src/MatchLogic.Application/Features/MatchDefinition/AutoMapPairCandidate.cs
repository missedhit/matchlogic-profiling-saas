using MatchLogic.Application.Interfaces.Comparator;
using MatchLogic.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.MatchDefinition
{
    /// <summary>
    /// Represents a pair candidate for auto-mapping.
    /// </summary>
    public class AutoMapPairCandidate
    {
        /// <summary>
        /// Gets the master field.
        /// </summary>
        public FieldMappingEx MasterField { get; }

        /// <summary>
        /// Gets the secondary field.
        /// </summary>
        public FieldMappingEx SecondaryField { get; }

        /// <summary>
        /// Gets the similarity score.
        /// </summary>
        public double Score { get; private set; }

        /// <summary>
        /// Creates a new instance of the AutoMapPairCandidate class.
        /// </summary>
        public AutoMapPairCandidate(FieldMappingEx masterField, FieldMappingEx secondaryField, IStringSimilarityCalculator stringSimilarityCalculator)
        {
            MasterField = masterField;
            SecondaryField = secondaryField;
            CalculateScore(stringSimilarityCalculator);
        }

        private void CalculateScore(IStringSimilarityCalculator stringSimilarityCalculator)
        {
            double typeSimilarity = 0;

            //// Check for transformation type match (if available)
            //// In practice, you would adapt this to your transformation types
            //if (MasterField.DataType == SecondaryField.DataType && !string.IsNullOrEmpty(MasterField.DataType))
            //{
            //    typeSimilarity = 10;
            //}

            // Use your string metric for name similarity
            double nameSimilarity = stringSimilarityCalculator.CalculateSimilarity(
                MasterField.FieldName.ToUpper(),
                SecondaryField.FieldName.ToUpper());

            // Apply slight adjustment based on field index for consistent sorting of equal scores
            nameSimilarity -= MasterField.FieldIndex * 1e-10;

            Score = typeSimilarity + nameSimilarity;
        }
    }
}
