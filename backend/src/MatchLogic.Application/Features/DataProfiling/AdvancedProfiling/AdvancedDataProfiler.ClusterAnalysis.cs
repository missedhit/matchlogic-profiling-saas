using MatchLogic.Domain.DataProfiling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling
{
    public partial class AdvancedDataProfiler
    {
        /// <summary>
        /// Get the representative value of a cluster for a specific column
        /// </summary>
        private string GetClusterRepresentative(RowCluster cluster, string columnName)
        {
            if (cluster.SampleRows.Count == 0)
                return string.Empty;

            // Try to find the most common value within the column
            var values = cluster.SampleRows
                .Where(r => r.ContainsKey(columnName) && r[columnName] != null)
                .GroupBy(r => r[columnName].ToString())
                .OrderByDescending(g => g.Count())
                .Select(g => g.Key)
                .FirstOrDefault();

            return values ?? string.Empty;
        }

        /// <summary>
        /// Get sample values of a cluster for a specific column
        /// </summary>
        private List<string> GetClusterSampleValues(RowCluster cluster, string columnName)
        {
            return cluster.SampleRows
                .Where(r => r.ContainsKey(columnName) && r[columnName] != null)
                .Select(r => r[columnName].ToString())
                .Distinct()
                .Take(5)
                .ToList();
        }

        /// <summary>
        /// Get sample row references of a cluster for a specific column
        /// </summary>
        private List<RowReference> GetClusterSampleRows(RowCluster cluster, string columnName)
        {
            var result = new List<RowReference>();

            for (int i = 0; i < Math.Min(cluster.SampleRows.Count, 5); i++)
            {
                var row = cluster.SampleRows[i];
                if (row.ContainsKey(columnName) && row[columnName] != null)
                {
                    result.Add(new RowReference
                    {
                        Value = row[columnName].ToString(),
                        RowData = new Dictionary<string, object>(row)
                    });
                }
            }

            return result;
        }
    }
}
