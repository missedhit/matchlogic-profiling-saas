using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using static MatchLogic.Application.Features.FinalExport.FinalScoresService;

namespace MatchLogic.Application.Interfaces.FinalExport;

public interface IFinalScoresService
{
    /// <summary>
    /// Gets transformed and flattened final scores data
    /// </summary>
    Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetTransformedFinalScoresAsync(FinalScoresFilter filter, int pageNumber, int pageSize);
    
    //Task GenerateFinalExportCollectionAsync(FinalScoresFilter filter);
    
    /// <summary>
    /// Generates a flattened final export collection
    /// </summary>
    Task GenerateFlattenedFinalExportCollectionAsync(FinalScoresFilter filter);
}