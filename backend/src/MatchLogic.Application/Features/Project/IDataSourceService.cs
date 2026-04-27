using MatchLogic.Domain.Project;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.Project
{
    public interface IDataSourceService
    {
        Task<DataSourcePreviewResult> PreviewDataSourceAsync(DataSource dataSource, CancellationToken cancellationToken);
        Task<ConnectionTestResult> TestConnectionAsync(DataSource dataSource, CancellationToken cancellationToken);
        Task<DataSourceMetadata> GetMetadataAsync(DataSource dataSource, CancellationToken cancellationToken);
        Task<DataSource> UpdateColumnMappingsAsync(DataSource dataSource, List<ColumnMappingRequest> mappings, CancellationToken cancellationToken);
    }
}
