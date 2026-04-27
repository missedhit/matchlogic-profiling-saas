using MatchLogic.Application.Interfaces.DataProfiling;
using MatchLogic.Application.Interfaces.Dictionary;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Application.Interfaces.Regex;
using MatchLogic.Domain.DataProfiling;
using MatchLogic.Domain.Project;
using MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;
using MatchLogic.Application.Features.DataProfiling;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.ML;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MatchLogic.Application.Features.DataProfiling.AdvancedProfiling;

public partial class AdvancedDataProfiler : IAdvancedDataProfiler
{
    private readonly ILogger<AdvancedDataProfiler> _logger;
    private readonly AdvancedProfilingOptions _defaultOptions;
    private readonly IRegexInfoService _regexService;
    private readonly IDictionaryCategoryService _dictionaryService;
    private readonly ITypeDetectionService _typeDetectionService;
    private readonly IDataQualityService _dataQualityService;
    private readonly IOutlierDetectionService _outlierDetectionService;
    private readonly IPatternDiscoveryService _patternDiscoveryService;
    private readonly IValidationRuleService _validationRuleService;
    private readonly IClusteringService _clusteringService;
    private readonly SemaphoreSlim _semaphore;
    private readonly ArrayPool<char> _charPool;
    private readonly MLContext _mlContext;
    private const string MetadataField = "_metadata";
    private const string RowNumberField = "RowNumber";
    private const string IdField = "_id";
    private readonly string[] SystemColumns = new string[] { IdField, MetadataField };
    private bool _disposed;
    private readonly IProfileRepository _profileRepository;

    /// <summary>
    /// Constructor with dependency injection
    /// </summary>
    public AdvancedDataProfiler(
        ILogger<AdvancedDataProfiler> logger,
        IOptions<AdvancedProfilingOptions> options,
        IRegexInfoService regexService,
        IDictionaryCategoryService dictionaryService,
        IProfileRepository profileRepository,
        ITypeDetectionService typeDetectionService = null,
        IDataQualityService dataQualityService = null,
        IOutlierDetectionService outlierDetectionService = null,
        IPatternDiscoveryService patternDiscoveryService = null,
        IValidationRuleService validationRuleService = null,
        IClusteringService clusteringService = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _defaultOptions = options?.Value ?? new AdvancedProfilingOptions();
        _regexService = regexService ?? throw new ArgumentNullException(nameof(regexService));
        _dictionaryService = dictionaryService ?? throw new ArgumentNullException(nameof(dictionaryService));
        _typeDetectionService = typeDetectionService;
        _dataQualityService = dataQualityService;
        _outlierDetectionService = outlierDetectionService;
        _patternDiscoveryService = patternDiscoveryService;
        _validationRuleService = validationRuleService;
        _clusteringService = clusteringService;
        _profileRepository = profileRepository;

        _semaphore = new SemaphoreSlim(_defaultOptions.MaxDegreeOfParallelism);
        _charPool = ArrayPool<char>.Shared;

        // Initialize ML.NET context
        _mlContext = new MLContext(seed: 42);
    }

    /// <summary>
    /// Profiles the provided data stream with advanced analytics capabilities
    /// </summary>
    public async Task<AdvancedProfileResult> ProfileDataAsync(
        IAsyncEnumerable<IDictionary<string, object>> dataStream,
        DataSource dataSource = null,
        IEnumerable<string> columnsToProfile = null,
        AdvancedProfilingOptions options = null,
        string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var startTime = DateTime.UtcNow;
        var effectiveOptions = options ?? _defaultOptions;

        var profileResult = new AdvancedProfileResult
        {
            ProfiledAt = startTime,
            DataSourceName = dataSource.Name,
            DataSourceId = dataSource.Id,
            AdvancedColumnProfiles = new ConcurrentDictionary<string, AdvancedColumnProfile>()
        };

        try
        {
            _logger.LogInformation("Starting advanced data profiling with {Features} enabled",
                GetEnabledFeatures(effectiveOptions));

            // Get all regex patterns from the service
            var regexPatterns = await _regexService.GetAllRegexInfo();
            var activeRegexes = regexPatterns
                .Where(r => !r.IsDeleted && r.IsDefault)
                .Select(r => (Id: r.Id, Name: r.Name, Pattern: new System.Text.RegularExpressions.Regex(r.RegexExpression, System.Text.RegularExpressions.RegexOptions.Compiled)))
                .ToList();

            // Get all dictionary categories
            var dictionaryCategories = await _dictionaryService.GetAllDictionaryCategories();
            var activeDictionaries = dictionaryCategories
                .Where(d => !d.IsDeleted && d.IsDefault)
                .Select(d => (Id: d.Id, Name: d.Name, Items: new HashSet<string>(d.Items, StringComparer.OrdinalIgnoreCase)))
                .ToList();

            // Create field analyzers for each column
            var fieldAnalyzers = new ConcurrentDictionary<string, AdvancedColumnAnalyzer>();
            var columnsToProfileSet = columnsToProfile?.ToHashSet(StringComparer.OrdinalIgnoreCase);

            // Store the raw data for row-level analysis if needed
            List<IDictionary<string, object>> rawData = null;
            if (effectiveOptions.EnableClustering || effectiveOptions.EnableOutlierDetection)
            {
                rawData = new List<IDictionary<string, object>>();
            }

            // Process the data using reactive extensions if enabled
            if (effectiveOptions.EnableReactivePipeline)
            {
                _logger.LogInformation("Processing data with Reactive Extensions pipeline");
                await ProcessDataWithReactiveExtensions(
                    dataStream,
                    fieldAnalyzers,
                    activeRegexes,
                    activeDictionaries,
                    columnsToProfileSet,
                    rawData,
                    effectiveOptions,
                    cancellationToken);
            }
            else
            {
                _logger.LogInformation("Processing data with channels for performance");
                await ProcessDataWithChannels(
                    dataStream,
                    fieldAnalyzers,
                    activeRegexes,
                    activeDictionaries,
                    columnsToProfileSet,
                    rawData,
                    effectiveOptions,
                    cancellationToken);
            }

            // Collections for row references
            var characteristicRowsByColumn = new Dictionary<string, Dictionary<ProfileCharacteristic, List<RowReference>>>();
            var patternRowsByColumn = new Dictionary<string, Dictionary<string, List<RowReference>>>();
            var valueRowsByColumn = new Dictionary<string, Dictionary<string, List<RowReference>>>();

            // Build advanced column profiles
            foreach (var analyzer in fieldAnalyzers)
            {
                var columnName = analyzer.Key;
                _logger.LogInformation("Building advanced profile for column '{ColumnName}'", columnName);
                // Convert row references
                var characteristicRows = ExtractCharacteristicRows(analyzer.Value);
                var patternRows = ExtractPatternRows(analyzer.Value);
                var valueRows = ExtractValueRows(analyzer.Value);

                // Store for saving
                characteristicRowsByColumn[columnName] = characteristicRows;
                patternRowsByColumn[columnName] = patternRows;
                valueRowsByColumn[columnName] = valueRows;

                var advancedProfile = await analyzer.Value.BuildEnhancedColumnProfileAsync(
                    effectiveOptions,
                    _typeDetectionService,
                    _outlierDetectionService,
                    _patternDiscoveryService,
                    _validationRuleService);

                profileResult.AdvancedColumnProfiles[analyzer.Key] = advancedProfile;
                _logger.LogInformation("Completed advanced profile for column '{ColumnName}' with {TotalCount} Filled Records",
                    columnName, advancedProfile.Filled);
            }

            // Set total record count
            profileResult.TotalRecords = fieldAnalyzers.Values.FirstOrDefault()?.TotalCount ?? 0;
            profileResult.ProfilingDuration = DateTime.UtcNow - startTime;

            // Perform cross-column analytics if enabled
            if (profileResult.TotalRecords > 0)
            {
                // Cross-column correlations if enabled
                if (effectiveOptions.EnableCorrelationAnalysis)
                {
                    _logger.LogInformation("Calculating correlation matrix for {Count} columns",
                        fieldAnalyzers.Count);
                    profileResult.CorrelationMatrix = await CalculateCorrelationMatrixAsync(
                        fieldAnalyzers,
                        effectiveOptions.MinCorrelationStrength);
                    _logger.LogInformation("Correlation matrix calculated with {Count} columns",
                        profileResult.CorrelationMatrix.Columns.Count);
                    // Detect column relationships based on correlation matrix
                    _logger.LogInformation("Detecting column relationships based on correlation matrix");
                    profileResult.ColumnRelationships = await DetectColumnRelationshipsAsync(
                        fieldAnalyzers,
                        profileResult.CorrelationMatrix);
                    _logger.LogInformation("Detected {Count} column relationships",
                        profileResult.ColumnRelationships.Count);

                }

                // Row-level clustering if enabled
                if (effectiveOptions.EnableClustering && _clusteringService != null && rawData != null && rawData.Count > 0)
                {
                    try
                    {
                        _logger.LogInformation("Performing row-level clustering on {Count} records",
                            rawData.Count);
                        var clusters = await _clusteringService.DetectRowClustersAsync(
                            rawData,
                            columnsToProfileSet,
                            effectiveOptions.MaxClusterCount,
                            cancellationToken);

                        // Add cluster info to individual columns
                        if (clusters.Count > 0)
                        {
                            foreach (var columnProfile in profileResult.AdvancedColumnProfiles.Values)
                            {
                                columnProfile.Clusters = clusters
                                    .Select(c => new ClusterInfo
                                    {
                                        ClusterId = c.ClusterId,
                                        Count = c.Count,
                                        Representative = GetClusterRepresentative(c, columnProfile.FieldName),
                                        SampleValues = GetClusterSampleValues(c, columnProfile.FieldName),
                                        SampleRows = GetClusterSampleRows(c, columnProfile.FieldName)
                                    })
                                    .ToList();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error performing row-level clustering");
                    }
                }

                // Data quality assessment if enabled                    
                if (effectiveOptions.EnableDataQualityAnalysis && _dataQualityService != null)
                {
                    _logger.LogInformation("Evaluating dataset quality for {Count} records",
                        profileResult.TotalRecords);
                    profileResult.DatasetQuality = await _dataQualityService.EvaluateDatasetQualityAsync(profileResult);
                }

                // Detect candidate keys and functional dependencies
                profileResult.CandidateKeys = await DetectCandidateKeysAsync(fieldAnalyzers);
                profileResult.FunctionalDependencies = await DetectFunctionalDependenciesAsync(fieldAnalyzers);

                // Generate recommendations based on profile results
                profileResult.Recommendations = await GenerateRecommendationsAsync(profileResult);
            }

            _logger.LogInformation("Completed advanced profiling of {Count} records in {Duration}",
                profileResult.TotalRecords, profileResult.ProfilingDuration);

            // Save profile result with row references
            _logger.LogInformation("Saving advanced profile result for collection '{CollectionName}'",
                collectionName ?? "DefaultCollection");
            await _profileRepository.SaveProfileResultAsync(
                profileResult,
            characteristicRowsByColumn,
                patternRowsByColumn,
                valueRowsByColumn, collectionName);
            _logger.LogInformation("Profile result saved successfully");
            return profileResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in advanced data profiling");
            throw;
        }
    }

    private async Task<CorrelationMatrix> CalculateCorrelationMatrixAsync(
        ConcurrentDictionary<string, AdvancedColumnAnalyzer> fieldAnalyzers,
        double minCorrelationStrength)
    {
        var matrix = new CorrelationMatrix();

        // Get numeric column analyzers
        var numericAnalyzers = fieldAnalyzers
            .Where(fa => fa.Value.HasNumericValues && fa.Value.NumericValues.ToList().Count > 1)
            .ToDictionary(kv => kv.Key, kv => kv.Value);

        if (numericAnalyzers.Count < 2)
        {
            _logger.LogInformation("Not enough numeric columns with sufficient values for correlation analysis");
            return matrix; // Not enough numeric columns for correlation
        }

        // Extract column names
        matrix.Columns = numericAnalyzers.Keys.ToList();

        // Create correlation matrix
        int columnCount = matrix.Columns.Count;
        matrix.Values = new double[columnCount, columnCount];

        _logger.LogInformation("Calculating correlation matrix for {Count} numeric columns", columnCount);

        // Use MathNet.Numerics to calculate correlations
        await Task.Run(() =>
        {
            for (int i = 0; i < columnCount; i++)
            {
                string colName1 = matrix.Columns[i];
                var analyzer1 = numericAnalyzers[colName1];

                // Self-correlation is always 1.0
                matrix.Values[i, i] = 1.0;

                for (int j = i + 1; j < columnCount; j++)
                {
                    string colName2 = matrix.Columns[j];
                    var analyzer2 = numericAnalyzers[colName2];

                    double correlation = 0;
                    try
                    {
                        // Ensure we have the same number of observations for correlation calculation
                        // by taking the intersection of non-null values

                        var values1 = analyzer1.NumericValues.ToList();
                        var values2 = analyzer2.NumericValues.ToList();

                        if (values1.Count != values2.Count && values1.Count > 0 && values2.Count > 0)
                        {
                            // Trim List to Equal Sizes
                            int minCount = Math.Min(values1.Count, values2.Count);
                            if (minCount > 1)
                            {
                                var commonValues1 = values1.Count == minCount ? values1 : values1.GetRange(0, minCount);
                                var commonValues2 = values2.Count == minCount ? values2 : values2.GetRange(0, minCount);

                                correlation = MathNet.Numerics.Statistics.Correlation.Pearson(
                                    commonValues1,
                                    commonValues2);
                            }

                        }
                        else
                        {
                            // Simple case: both columns have the same number of observations
                            // Assume they're from the same rows
                            correlation = MathNet.Numerics.Statistics.Correlation.Pearson(
                                analyzer1.NumericValues.ToArray(),
                                analyzer2.NumericValues.ToArray());
                        }
                    }
                    catch (Exception ex)
                    {
                        // If correlation calculation fails, set to 0
                        _logger.LogWarning(ex, "Correlation calculation failed for columns {Column1} and {Column2}", colName1, colName2);
                        correlation = 0;
                    }

                    // Set correlation in matrix
                    matrix.Values[i, j] = correlation;
                    matrix.Values[j, i] = correlation; // Matrix is symmetric

                    if (Math.Abs(correlation) >= minCorrelationStrength)
                    {
                        _logger.LogDebug("Strong correlation ({Correlation:F2}) found between {Column1} and {Column2}",
                            correlation, colName1, colName2);
                    }
                }
            }
        });

        _logger.LogInformation("Completed correlation matrix calculation");
        return matrix;
    }

    private async Task<List<ColumnRelationship>> DetectColumnRelationshipsAsync(
        ConcurrentDictionary<string, AdvancedColumnAnalyzer> fieldAnalyzers,
        CorrelationMatrix correlationMatrix)
    {
        if (correlationMatrix?.Columns == null || correlationMatrix.Columns.Count < 2)
        {
            _logger.LogInformation("No correlation matrix available for relationship detection");
            return new List<ColumnRelationship>();
        }

        var relationships = new List<ColumnRelationship>();

        _logger.LogInformation("Detecting column relationships from correlation matrix with {Count} columns",
            correlationMatrix.Columns.Count);

        await Task.Run(() =>
        {
            int columnCount = correlationMatrix.Columns.Count;

            // First pass: Extract direct correlations from the matrix
            for (int i = 0; i < columnCount; i++)
            {
                for (int j = i + 1; j < columnCount; j++)
                {
                    double correlation = correlationMatrix.Values[i, j];

                    // Only include correlations above the threshold
                    if (Math.Abs(correlation) >= _defaultOptions.MinCorrelationStrength)
                    {
                        string relType = DetermineRelationshipType(correlation);

                        relationships.Add(new ColumnRelationship
                        {
                            SourceColumn = correlationMatrix.Columns[i],
                            TargetColumn = correlationMatrix.Columns[j],
                            RelationshipType = relType,
                            Strength = Math.Abs(correlation)
                        });
                    }
                }
            }

            // Second pass: Look for potential transitive relationships
            // A → B and B → C might suggest A → C
            if (_defaultOptions.DetectTransitiveRelationships && relationships.Count > 1)
            {
                var transitiveRelationships = new List<ColumnRelationship>();

                foreach (var rel1 in relationships)
                {
                    foreach (var rel2 in relationships)
                    {
                        // Check if rel1.Target = rel2.Source
                        if (rel1.TargetColumn == rel2.SourceColumn &&
                            rel1.SourceColumn != rel2.TargetColumn &&
                            rel1.Strength > 0.8 && rel2.Strength > 0.8)
                        {
                            // Check if direct relationship already exists
                            bool directExists = relationships.Any(r =>
                                (r.SourceColumn == rel1.SourceColumn && r.TargetColumn == rel2.TargetColumn) ||
                                (r.SourceColumn == rel2.TargetColumn && r.TargetColumn == rel1.SourceColumn)
                            );

                            if (!directExists)
                            {
                                // Calculate expected transitive strength
                                double transitiveStrength = rel1.Strength * rel2.Strength;

                                // Only add if strong enough
                                if (transitiveStrength >= _defaultOptions.MinCorrelationStrength)
                                {
                                    transitiveRelationships.Add(new ColumnRelationship
                                    {
                                        SourceColumn = rel1.SourceColumn,
                                        TargetColumn = rel2.TargetColumn,
                                        RelationshipType = "Transitive",
                                        Strength = transitiveStrength
                                    });

                                    _logger.LogDebug("Detected transitive relationship: {Source} → {Intermediate} → {Target}",
                                        rel1.SourceColumn, rel1.TargetColumn, rel2.TargetColumn);
                                }
                            }
                        }
                    }
                }

                // Add transitive relationships to the results
                relationships.AddRange(transitiveRelationships);
            }

            // Sort by strength
            relationships = relationships.OrderByDescending(r => r.Strength).ToList();
        });

        _logger.LogInformation("Detected {Count} column relationships", relationships.Count);
        return relationships;
    }

    /// <summary>
    /// Determine the type of relationship based on correlation value
    /// </summary>
    private string DetermineRelationshipType(double correlation)
    {
        if (correlation > 0.95)
        {
            return "OneToOne";
        }
        else if (correlation < -0.95)
        {
            return "InverseOneToOne";
        }
        else if (correlation > 0.85)
        {
            return "StrongPositive";
        }
        else if (correlation < -0.85)
        {
            return "StrongNegative";
        }
        else if (correlation > 0.7)
        {
            return "ModeratePositive";
        }
        else if (correlation < -0.7)
        {
            return "ModerateNegative";
        }
        else
        {
            return "Weak";
        }
    }

    private async Task<List<CandidateKey>> DetectCandidateKeysAsync(
        ConcurrentDictionary<string, AdvancedColumnAnalyzer> fieldAnalyzers)
    {
        var candidateKeys = new List<CandidateKey>();

        _logger.LogInformation("Starting candidate key detection");

        // Get column uniqueness values
        var uniqueColumns = fieldAnalyzers
            .Where(fa => fa.Value.NonNullCount > 0)
            .Select(fa => new
            {
                ColumnName = fa.Key,
                Uniqueness = fa.Value.UniqueValuePercentage,
                NonNullCount = fa.Value.NonNullCount,
                NullCount = fa.Value.TotalCount - fa.Value.NonNullCount,
                TotalCount = fa.Value.TotalCount
            })
            .Where(col => col.Uniqueness > 0.5) // Only consider columns with reasonable uniqueness
            .OrderByDescending(col => col.Uniqueness)
            .ThenByDescending(col => col.NonNullCount)
            .ToList();

        _logger.LogDebug("Found {Count} columns with uniqueness > 0.5", uniqueColumns.Count);

        if (uniqueColumns.Count == 0)
        {
            _logger.LogInformation("No columns with sufficient uniqueness found for key detection");
            return candidateKeys;
        }

        await Task.Run(() =>
        {
            // Single-column candidate keys
            foreach (var col in uniqueColumns)
            {
                // Calculate adjusted uniqueness score that penalizes null values
                double nullPenalty = (double)col.NullCount / col.TotalCount * 0.2; // 20% penalty for null ratio
                double adjustedUniqueness = Math.Max(0, col.Uniqueness - nullPenalty);

                candidateKeys.Add(new CandidateKey
                {
                    Columns = new List<string> { col.ColumnName },
                    Uniqueness = adjustedUniqueness,
                    NonNullCount = (int)col.NonNullCount
                });

                _logger.LogDebug("Added single-column key candidate: {Column} with uniqueness {Uniqueness:F2}",
                    col.ColumnName, adjustedUniqueness);
            }

            // Multi-column candidate keys
            if (uniqueColumns.Count >= 2)
            {
                // Find high-quality columns for composite keys (top 5 or fewer)
                var keyColumnCandidates = uniqueColumns
                    .Take(Math.Min(5, uniqueColumns.Count))
                    .ToList();

                // Try two-column combinations
                for (int i = 0; i < keyColumnCandidates.Count; i++)
                {
                    for (int j = i + 1; j < keyColumnCandidates.Count; j++)
                    {
                        var col1 = keyColumnCandidates[i];
                        var col2 = keyColumnCandidates[j];

                        // Only consider combinations where both columns have decent uniqueness
                        if (col1.Uniqueness > 0.5 && col2.Uniqueness > 0.5)
                        {
                            // Estimate composite uniqueness
                            // This is a heuristic - in a real implementation we'd examine actual data combinations
                            double combinedUniqueness = EstimateCompositeUniqueness(col1.Uniqueness, col2.Uniqueness);

                            candidateKeys.Add(new CandidateKey
                            {
                                Columns = new List<string> { col1.ColumnName, col2.ColumnName },
                                Uniqueness = combinedUniqueness,
                                NonNullCount = (int)Math.Min(col1.NonNullCount, col2.NonNullCount)
                            });

                            _logger.LogDebug("Added composite key candidate: {Column1}+{Column2} with uniqueness {Uniqueness:F2}",
                                col1.ColumnName, col2.ColumnName, combinedUniqueness);
                        }
                    }
                }

                // For datasets with many columns, try three-column combinations for the top candidates
                if (keyColumnCandidates.Count >= 3 && uniqueColumns.Count > 5)
                {
                    // Take top 3 candidates for three-column keys to avoid combinatorial explosion
                    var topCandidates = keyColumnCandidates.Take(3).ToList();

                    for (int i = 0; i < topCandidates.Count; i++)
                    {
                        for (int j = i + 1; j < topCandidates.Count; j++)
                        {
                            for (int k = j + 1; k < topCandidates.Count; k++)
                            {
                                var col1 = topCandidates[i];
                                var col2 = topCandidates[j];
                                var col3 = topCandidates[k];

                                // Estimate three-column uniqueness
                                double combinedUniqueness = EstimateTripleCompositeUniqueness(
                                    col1.Uniqueness, col2.Uniqueness, col3.Uniqueness);

                                candidateKeys.Add(new CandidateKey
                                {
                                    Columns = new List<string> { col1.ColumnName, col2.ColumnName, col3.ColumnName },
                                    Uniqueness = combinedUniqueness,
                                    NonNullCount = (int)Math.Min(Math.Min(col1.NonNullCount, col2.NonNullCount), col3.NonNullCount)
                                });

                                _logger.LogDebug("Added triple composite key candidate: {Column1}+{Column2}+{Column3} with uniqueness {Uniqueness:F2}",
                                    col1.ColumnName, col2.ColumnName, col3.ColumnName, combinedUniqueness);
                            }
                        }
                    }
                }
            }

            // Sort all candidates by uniqueness (highest first)
            candidateKeys = candidateKeys
                .OrderByDescending(k => k.Uniqueness)
                .ThenBy(k => k.Columns.Count) // Prefer simpler keys when uniqueness is similar
                .ToList();
        });

        _logger.LogInformation("Detected {Count} candidate keys", candidateKeys.Count);
        return candidateKeys;
    }

    /// <summary>
    /// Estimate the uniqueness of a composite key from two columns
    /// </summary>
    private double EstimateCompositeUniqueness(double uniqueness1, double uniqueness2)
    {
        // This is a heuristic formula - a perfect implementation would examine actual value combinations
        // The formula aims to estimate how combining columns improves uniqueness

        // If one column is already fully unique, the composite key is also fully unique
        if (uniqueness1 >= 0.999 || uniqueness2 >= 0.999)
            return 0.999;

        // Calculate joint probability of collision
        // P(collision in both) = P(collision in 1) * P(collision in 2)
        double collisionProb1 = 1 - uniqueness1;
        double collisionProb2 = 1 - uniqueness2;

        // Assume some correlation between columns (completely independent columns would use: collisionProb1 * collisionProb2)
        // We use a conservative 0.7 correlation factor
        double jointCollisionProb = collisionProb1 * collisionProb2 * 0.7;

        // Convert back to uniqueness
        double estimatedUniqueness = 1 - jointCollisionProb;

        // Cap at 0.999 to avoid overly optimistic estimates
        return Math.Min(0.999, estimatedUniqueness);
    }

    /// <summary>
    /// Estimate the uniqueness of a composite key from three columns
    /// </summary>
    private double EstimateTripleCompositeUniqueness(double uniqueness1, double uniqueness2, double uniqueness3)
    {
        // Similar to two-column estimation but with three columns

        // If any column is already fully unique, the composite key is also fully unique
        if (uniqueness1 >= 0.999 || uniqueness2 >= 0.999 || uniqueness3 >= 0.999)
            return 0.999;

        double collisionProb1 = 1 - uniqueness1;
        double collisionProb2 = 1 - uniqueness2;
        double collisionProb3 = 1 - uniqueness3;

        // Apply a more conservative correlation factor for three columns (0.5)
        double jointCollisionProb = collisionProb1 * collisionProb2 * collisionProb3 * 0.5;

        // Convert back to uniqueness and cap at 0.999
        return Math.Min(0.999, 1 - jointCollisionProb);
    }

    private async Task<List<FunctionalDependency>> DetectFunctionalDependenciesAsync(
        ConcurrentDictionary<string, AdvancedColumnAnalyzer> fieldAnalyzers)
    {
        var dependencies = new List<FunctionalDependency>();

        _logger.LogInformation("Starting functional dependency detection");

        // Group analyzers by type for more focused analysis
        var numericAnalyzers = fieldAnalyzers
            .Where(fa => fa.Value.HasNumericValues)
            .ToDictionary(kv => kv.Key, kv => kv.Value);

        var categoricalAnalyzers = fieldAnalyzers
            .Where(fa => !fa.Value.HasNumericValues && fa.Value.NumericValues.Distinct().ToList().Count < fieldAnalyzers[fa.Key].TotalCount * 0.5)
            .ToDictionary(kv => kv.Key, kv => kv.Value);

        var identifierAnalyzers = fieldAnalyzers
            .Where(fa => fa.Value.UniqueValuePercentage > 0.9)
            .ToDictionary(kv => kv.Key, kv => kv.Value);

        _logger.LogDebug("Found {NumericCount} numeric columns, {CategoricalCount} categorical columns, and {IdentifierCount} identifier columns",
            numericAnalyzers.Count, categoricalAnalyzers.Count, identifierAnalyzers.Count);

        if (numericAnalyzers.Count < 2 && categoricalAnalyzers.Count < 2)
        {
            _logger.LogInformation("Not enough columns for functional dependency detection");
            return dependencies;
        }

        await Task.Run(() =>
        {
            // Numeric-to-numeric dependencies based on correlation
            if (numericAnalyzers.Count >= 2)
            {
                var columns = numericAnalyzers.Keys.ToList();
                int columnCount = columns.Count;

                for (int i = 0; i < columnCount; i++)
                {
                    for (int j = 0; j < columnCount; j++)
                    {
                        if (i == j) continue;


                        var analyzer1 = numericAnalyzers[columns[i]];
                        var analyzer2 = numericAnalyzers[columns[j]];

                        double correlation = 0;
                        try
                        {
                            var values1 = analyzer1.NumericValues.ToList();
                            var values2 = analyzer2.NumericValues.ToList();

                            if (values1.Count != values2.Count && values1.Count > 0 && values2.Count > 0)
                            {
                                // Trim List to Equal Sizes
                                int minCount = Math.Min(values1.Count, values2.Count);
                                if (minCount > 1)
                                {
                                    var commonValues1 = values1.Count == minCount ? values1 : values1.GetRange(0, minCount);
                                    var commonValues2 = values2.Count == minCount ? values2 : values2.GetRange(0, minCount);

                                    correlation = MathNet.Numerics.Statistics.Correlation.Pearson(
                                        commonValues1,
                                        commonValues2);
                                }

                            }
                            else
                            {
                                // Simple case: both columns have the same number of observations
                                // Assume they're from the same rows
                                correlation = MathNet.Numerics.Statistics.Correlation.Pearson(
                                    analyzer1.NumericValues.ToArray(),
                                    analyzer2.NumericValues.ToArray());
                            }
                        }
                        catch
                        {
                            correlation = 0;
                        }

                        // Strong correlation might indicate functional dependency
                        if (correlation > 0.9)
                        {
                            // Estimate if the relationship is deterministic (true functional dependency)
                            // based on value counts
                            double confidenceScore = EstimateFunctionalDependencyConfidence(analyzer1, analyzer2);

                            if (confidenceScore > 0.8)
                            {
                                dependencies.Add(new FunctionalDependency
                                {
                                    DeterminantColumns = new List<string> { columns[i] },
                                    DependentColumns = new List<string> { columns[j] },
                                    Confidence = confidenceScore,
                                    DependencyType = "Numeric"
                                });

                                _logger.LogDebug("Detected numeric functional dependency: {Determinant} → {Dependent} with confidence {Confidence:F2}",
                                    columns[i], columns[j], confidenceScore);
                            }
                        }
                    }
                }
            }

            // Categorical-to-categorical dependencies
            if (categoricalAnalyzers.Count >= 2)
            {
                var columns = categoricalAnalyzers.Keys.ToList();

                // For each potential determinant column
                foreach (var determinantCol in columns)
                {
                    var determinantAnalyzer = categoricalAnalyzers[determinantCol];

                    // Skip if too many unique values (not a good determinant)
                    if (determinantAnalyzer.UniqueValuePercentage > 0.8)
                        continue;

                    // For each potential dependent column
                    foreach (var dependentCol in columns)
                    {
                        if (determinantCol == dependentCol) continue;

                        var dependentAnalyzer = categoricalAnalyzers[dependentCol];

                        // Check if determinant → dependent is plausible
                        // Typically, determinants have fewer unique values than dependents
                        if (determinantAnalyzer.UniqueValuePercentage <= dependentAnalyzer.UniqueValuePercentage)
                        {
                            // In a full implementation, we would analyze the value distributions
                            // to check if each determinant value maps to a single dependent value

                            // Here we use a heuristic based on uniqueness ratios
                            double confidenceScore = EstimateCategoricalDependencyConfidence(
                                determinantAnalyzer, dependentAnalyzer);

                            if (confidenceScore > 0.7)
                            {
                                dependencies.Add(new FunctionalDependency
                                {
                                    DeterminantColumns = new List<string> { determinantCol },
                                    DependentColumns = new List<string> { dependentCol },
                                    Confidence = confidenceScore,
                                    DependencyType = "Categorical"
                                });

                                _logger.LogDebug("Detected categorical functional dependency: {Determinant} → {Dependent} with confidence {Confidence:F2}",
                                    determinantCol, dependentCol, confidenceScore);
                            }
                        }
                    }
                }
            }

            // Check for identifier-to-attribute dependencies
            if (identifierAnalyzers.Count > 0)
            {
                foreach (var identifierCol in identifierAnalyzers.Keys)
                {
                    var identifierAnalyzer = identifierAnalyzers[identifierCol];

                    // If this is a high-quality identifier (very high uniqueness)
                    if (identifierAnalyzer.UniqueValuePercentage > 0.95)
                    {
                        // It likely determines most other non-identifier columns
                        foreach (var col in fieldAnalyzers.Keys)
                        {
                            if (col != identifierCol && !identifierAnalyzers.ContainsKey(col))
                            {
                                dependencies.Add(new FunctionalDependency
                                {
                                    DeterminantColumns = new List<string> { identifierCol },
                                    DependentColumns = new List<string> { col },
                                    Confidence = 0.95,
                                    DependencyType = "Identifier"
                                });

                                _logger.LogDebug("Detected identifier-based dependency: {Identifier} → {Dependent}",
                                    identifierCol, col);
                            }
                        }
                    }
                }
            }

            // Multi-column dependencies (simplified approach)
            // In a real implementation, we'd use algorithms like TANE or FD_Mine

            // Sort by confidence
            dependencies = dependencies
                .OrderByDescending(d => d.Confidence)
                .ThenBy(d => d.DeterminantColumns.Count) // Prefer simpler dependencies
                .ToList();
        });

        _logger.LogInformation("Detected {Count} functional dependencies", dependencies.Count);
        return dependencies;
    }

    /// <summary>
    /// Estimate the confidence of a functional dependency between two numeric columns
    /// </summary>
    private double EstimateFunctionalDependencyConfidence(
        AdvancedColumnAnalyzer determinantAnalyzer,
        AdvancedColumnAnalyzer dependentAnalyzer)
    {
        // This is a heuristic - in a real implementation, we'd analyze if each determinant value
        // maps to a unique dependent value

        // Basic correlation component (0-0.8)
        double correlationComponent = 0;
        try
        {
            double correlation = MathNet.Numerics.Statistics.Correlation.Pearson(
                determinantAnalyzer.NumericValues.ToArray(),
                dependentAnalyzer.NumericValues.ToArray());

            correlationComponent = Math.Abs(correlation) * 0.8;
        }
        catch
        {
            correlationComponent = 0;
        }

        // Unique value ratio component (0-0.2)
        // If determinant has unique values similar to dependent, more likely to be functional
        double uniqueRatio = (double)determinantAnalyzer.UniqueValuePercentage / Math.Max(1, dependentAnalyzer.UniqueValuePercentage);
        double uniqueComponent = uniqueRatio * 0.2;

        // Combine components
        return Math.Min(0.99, correlationComponent + uniqueComponent);
    }

    /// <summary>
    /// Estimate the confidence of a functional dependency between two categorical columns
    /// </summary>
    private double EstimateCategoricalDependencyConfidence(
        AdvancedColumnAnalyzer determinantAnalyzer,
        AdvancedColumnAnalyzer dependentAnalyzer)
    {
        // Simple heuristic based on uniqueness
        // In a real implementation, we'd check value-to-value mappings

        // If determinant has more unique values than dependent, it can't be a determinant
        if (determinantAnalyzer.UniqueValuePercentage > dependentAnalyzer.UniqueValuePercentage)
            return 0;

        // Calculate ratio of unique values
        double uniqueRatio = (double)determinantAnalyzer.UniqueValuePercentage / Math.Max(1, dependentAnalyzer.UniqueValuePercentage);

        // Closer to 1.0 means more likely to be functional
        double baseConfidence = 0.7 + (uniqueRatio * 0.2);

        // Additional confidence if null counts match
        //double nullSimilarity = Math.Min((determinantAnalyzer.TotalCount - determinantAnalyzer.NonNullCount), (dependentAnalyzer.TotalCount - dependentAnalyzer.NonNullCount)) /
        //                       Math.Max((determinantAnalyzer.TotalCount - determinantAnalyzer.NonNullCount), (dependentAnalyzer.TotalCount - dependentAnalyzer.NonNullCount));
        // Make sure divisor is not zero
        double nullSimilarity = (Math.Max((determinantAnalyzer.TotalCount - determinantAnalyzer.NonNullCount), (dependentAnalyzer.TotalCount - dependentAnalyzer.NonNullCount)) == 0)
        ? 0
        : Math.Min((determinantAnalyzer.TotalCount - determinantAnalyzer.NonNullCount), (dependentAnalyzer.TotalCount - dependentAnalyzer.NonNullCount)) /
          Math.Max((determinantAnalyzer.TotalCount - determinantAnalyzer.NonNullCount), (dependentAnalyzer.TotalCount - dependentAnalyzer.NonNullCount));

        double nullComponent = nullSimilarity * 0.1;

        return Math.Min(0.95, baseConfidence + nullComponent);
    }


    /// <summary>
    /// Extracts characteristic rows from a column analyzer
    /// </summary>
    private Dictionary<ProfileCharacteristic, List<RowReference>> ExtractCharacteristicRows(AdvancedColumnAnalyzer analyzer)
    {
        var characteristicRows = new Dictionary<ProfileCharacteristic, List<RowReference>>();

        foreach (var characteristicPair in analyzer._characteristicRows)
        {
            characteristicRows[characteristicPair.Key] = characteristicPair.Value
                .Take(_defaultOptions.MaxRowsPerCategory)
                .ToList();
        }

        return characteristicRows;
    }

    /// <summary>
    /// Extracts pattern rows from a column analyzer
    /// </summary>
    private Dictionary<string, List<RowReference>> ExtractPatternRows(AdvancedColumnAnalyzer analyzer)
    {
        var patternRows = new Dictionary<string, List<RowReference>>();

        foreach (var patternPair in analyzer._patternStats)
        {
            patternRows[$"{patternPair.Key}_Valid"] = patternPair.Value.ValidRows
                .Take(_defaultOptions.MaxRowsPerCategory)
                .ToList();

            patternRows[$"{patternPair.Key}_Invalid"] = patternPair.Value.InvalidRows
                .Take(_defaultOptions.MaxRowsPerCategory)
                .ToList();
        }

        return patternRows;
    }

    /// <summary>
    /// Extracts value rows from a column analyzer
    /// </summary>
    private Dictionary<string, List<RowReference>> ExtractValueRows(AdvancedColumnAnalyzer analyzer)
    {
        var valueRows = new Dictionary<string, List<RowReference>>();

        foreach (var valuePair in analyzer._valueRows)
        {
            valueRows[valuePair.Key] = valuePair.Value
                .Take(_defaultOptions.MaxRowsPerCategory)
                .ToList();
        }

        return valueRows;
    }
}
