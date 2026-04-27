using MatchLogic.Application.Common;
using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.Transform;
using MatchLogic.Application.Interfaces.FinalExport;
using MatchLogic.Application.Interfaces.MatchConfiguration;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Transform;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.FinalExport;
public class FinalScoresService : IFinalScoresService
{
    private readonly IDataStore _dataStore;
    private readonly IAutoMappingService _mappingService;
    private readonly IGenericRepository<Domain.Project.DataSource, Guid> _dataSourceRepository;
    private readonly IDataTransformerFactory _groupDataTransformerFactory;

    public FinalScoresService(
        IDataStore dataStore,
        IAutoMappingService mappingService,
        IGenericRepository<Domain.Project.DataSource, Guid> dataSourceRepository,
        IDataTransformerFactory groupDataTransformerFactory)
    {
        _dataStore = dataStore;
        _mappingService = mappingService;
        _dataSourceRepository = dataSourceRepository;
        _groupDataTransformerFactory = groupDataTransformerFactory;
    }

    public record FinalScoresFilter
    {
        public Guid ProjectId { get; set; }
        public Guid[] DataSourceGuids { get; set; } = Array.Empty<Guid>();
        public bool? Selected { get; set; }
        public bool? NotDuplicate { get; set; }
        public bool? Master { get; set; }
        public DuplicateExportMode ExportMode { get; set; } = DuplicateExportMode.AllRecords;
    }

    public enum DuplicateExportMode
    {
        AllRecords,
        UniqueOnly,
        MasterOnly,
        DuplicatesOnly,
        CrossReference
    }

    private async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetFinalScoresAsync(FinalScoresFilter filter, int pageNumber, int pageSize)
    {
        var collectionName = "groups_" + GuidCollectionNameConverter.ToValidCollectionName(filter.ProjectId);
        var filterBuilder = new List<string>();

        // Filter by selected databases (datasets)
        if (filter.DataSourceGuids != null && filter.DataSourceGuids.Length > 0)
        {
            var neighborFilter = "Records._group_match_details.neighbor~" +
                string.Join("|", filter.DataSourceGuids.Select(g => g + "%"));
            filterBuilder.Add(neighborFilter);
        }

        // Filter by Selected
        if (filter.Selected.HasValue)
            filterBuilder.Add($"Selected={filter.Selected.Value.ToString().ToLower()}");
        /*        // Filter by NotDuplicate
        if (filter.NotDuplicate.HasValue)
            filterBuilder.Add($"NotDuplicate={filter.NotDuplicate.Value.ToString().ToLower()}");

        // Filter by Master
        if (filter.Master.HasValue)
            filterBuilder.Add($"Master={filter.Master.Value.ToString().ToLower()}");*/
        // Handle export mode
        switch (filter.ExportMode)
        {
            case DuplicateExportMode.AllRecords:
                // No additional filter
                break;
            case DuplicateExportMode.UniqueOnly:
                filterBuilder.Add("NotDuplicate=true");
                break;
            case DuplicateExportMode.MasterOnly:
                filterBuilder.Add("Master=true");
                break;
            case DuplicateExportMode.DuplicatesOnly:
                filterBuilder.Add("NotDuplicate=false");
                break;
            case DuplicateExportMode.CrossReference:
                throw new NotImplementedException("CrossReference mode is not implemented yet.");
                // Special handling: fetch all, then filter groups with >1 dataset
                /*var (allItems, _) = await _dataStore.GetPagedWithSmartFilteringAndProjectionAsync(
                    collectionName, 1, int.MaxValue, filters: string.Join(";", filterBuilder));
                // Group by GroupId and filter groups with >1 unique neighbor
                var crossRefGroups = allItems
                    .GroupBy(x => x.ContainsKey("GroupId") ? x["GroupId"] : null)
                    .Where(g =>
                        g.Select(r =>
                        {
                            // Try to get all neighbor guids from nested structure
                            if (r.TryGetValue("Records", out var recordsObj) && recordsObj is IEnumerable<object> records)
                            {
                                var neighbors = new HashSet<string>();
                                foreach (var rec in records)
                                {
                                    if (rec is IDictionary<string, object> recDict &&
                                        recDict.TryGetValue("_group_match_details", out var detailsObj) &&
                                        detailsObj is IEnumerable<object> details)
                                    {
                                        foreach (var detail in details)
                                        {
                                            if (detail is IDictionary<string, object> detailDict &&
                                                detailDict.TryGetValue("neighbor", out var neighborObj) &&
                                                neighborObj is string neighborStr)
                                            {
                                                neighbors.Add(neighborStr);
                                            }
                                        }
                                    }
                                }
                                return neighbors;
                            }
                            return Enumerable.Empty<string>();
                        })
                        .SelectMany(x => x)
                        .Select(n => n.Split(':').FirstOrDefault() ?? n) // In case neighbor is "guid:..."
                        .Distinct()
                        .Count() > 1
                    );
                return crossRefGroups.SelectMany(g => g);*/
        }

        var finalFilter = string.Join(";", filterBuilder);

        var (items, count) = await _dataStore.GetPagedWithSmartFilteringAndProjectionAsync(
            collectionName,
            pageNumber,
            pageSize,
            filters: finalFilter);

        return (items, count);
    }

    /// <summary>
    /// Gets transformed final scores data that's flattened for export or preview
    /// </summary>
    public async Task<(IEnumerable<IDictionary<string, object>> Data, int TotalCount)> GetTransformedFinalScoresAsync(FinalScoresFilter filter, int pageNumber, int pageSize)
    {

        var (rawData, totalCount) = await GetFinalScoresAsync(filter, pageNumber, pageSize);

        if (rawData == null)
            return (Enumerable.Empty<IDictionary<string, object>>(), 0);

        var fieldMappings = await _mappingService.GetSavedMappedFieldRowsAsync(filter.ProjectId);
        var dataSourceDict = (await _dataSourceRepository.QueryAsync(ds => ds.ProjectId == filter.ProjectId, Constants.Collections.DataSources))
            .ToDictionary(ds => ds.Id, ds => ds.Name);

        var config = new TransformerConfiguration
        {
            TransformerType = "groups",
            Settings = new Dictionary<string, object>
            {
                ["fieldMappings"] = fieldMappings,
                ["dataSourceDict"] = dataSourceDict
            }
        };

        // Try to estimate capacity to reduce List resizes.
        //int estimatedInputCount = 0;
        //if (rawData is ICollection<IDictionary<string, object>> coll)
        //    estimatedInputCount = coll.Count;
        //else if (rawData is ICollection nonGenericColl)
        //    estimatedInputCount = nonGenericColl.Count;

        int initialCapacity = Math.Max(16, totalCount);

        // Minimal allocation wrapper to stream IEnumerable -> IAsyncEnumerable without external LINQ allocations.
        static async IAsyncEnumerable<IDictionary<string, object>> EnumerableToAsync(IEnumerable<IDictionary<string, object>> src)
        {
            foreach (var item in src)
            {
                yield return item;
                // No await here to avoid extra scheduling; async iterator still valid.
            }
        }

        var transformedRows = new List<IDictionary<string, object>>(initialCapacity);

        // Get transformer and ensure disposal when done.
        using (var groupTransformer = _groupDataTransformerFactory.GetTransformer(config))
        {
            // Stream transform without materializing the input.
            await foreach (var row in groupTransformer.TransformAsync(EnumerableToAsync(rawData)))
            {
                transformedRows.Add(row);
            }
        }

        return (transformedRows, totalCount);
    }

    /// <summary>
    /// Generates a flattened final export collection for the given filter
    /// </summary>
    public async Task GenerateFlattenedFinalExportCollectionAsync(FinalScoresFilter filter)
    {
        var finalExportCollectionName = "finalExport_" + GuidCollectionNameConverter.ToValidCollectionName(filter.ProjectId);

        // Delete existing collection (idempotent for repeated runs)
        await _dataStore.DeleteCollection(finalExportCollectionName);

        // Fetch mapping metadata once (expensive operations done outside loop)
        var fieldMappings = await _mappingService.GetSavedMappedFieldRowsAsync(filter.ProjectId);
        var dataSourceDict = (await _dataSourceRepository.QueryAsync(ds => ds.ProjectId == filter.ProjectId, Constants.Collections.DataSources))
            .ToDictionary(ds => ds.Id, ds => ds.Name);

        // Build immutable transformer configuration once
        var config = new TransformerConfiguration
        {
            TransformerType = "groups",
            Settings = new Dictionary<string, object>
            {
                ["fieldMappings"] = fieldMappings,
                ["dataSourceDict"] = dataSourceDict
            }
        };

        const int batchSize = 100;
        var currentPage = 1;

        // Single streaming adapter to convert IEnumerable -> IAsyncEnumerable with no extra allocations.
        static async IAsyncEnumerable<IDictionary<string, object>> EnumerableToAsync(IEnumerable<IDictionary<string, object>> src)
        {
            foreach (var item in src)
                yield return item;
        }

        // Get one transformer and reuse across pages to avoid repeated allocations and setup.
        // The documentation states transformers are stateless and thread-safe; reusing sequentially is valid.
        using var groupTransformer = _groupDataTransformerFactory.GetTransformer(config);

        while (true)
        {
            // Fetch raw page. GetFinalScoresAsync returns an IEnumerable that may be lazily enumerated.
            var (rawBatchEnumerable, totalCount) = await GetFinalScoresAsync(filter, currentPage, batchSize);

            // Materialize once into a List to avoid double enumeration (Any/Count/foreach).
            // If the underlying source is already an IList, avoid copying.
            var rawBatch = rawBatchEnumerable as IList<IDictionary<string, object>> ?? rawBatchEnumerable.ToList();

            // If no records in this page, exit loop. Single materialization ensures we don't enumerate twice.
            if (rawBatch.Count == 0)
                break;

            // Pre-size transformed list using known input size to minimize internal resizes and GC churn.
            var transformedRows = new List<IDictionary<string, object>>(Math.Max(16, rawBatch.Count));

            // Stream transform the page using the reused transformer. This avoids materializing full result sets
            // inside the transformer and keeps memory per-row.
            await foreach (var row in groupTransformer.TransformAsync(EnumerableToAsync(rawBatch)))
            {
                transformedRows.Add(row);
            }

            // Insert only when there is data. Use a single bulk insert per transformed page to minimize I/O calls.
            if (transformedRows.Count > 0)
            {
                await _dataStore.InsertBatchAsync(finalExportCollectionName, transformedRows);
            }

            // If this page had fewer items than batchSize, it's the last page.
            if (rawBatch.Count < batchSize)
                break;

            currentPage++;
        }
    }
}


