using MatchLogic.Application.Interfaces.Cleansing;
using MatchLogic.Domain.CleansingAndStandaradization;
using MatchLogic.Infrastructure.CleansingAndStandardization.Enhance;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization;

/// <summary>
/// Context for applying transformations to records
/// </summary>
public class TransformationContext : ITransformationContext
{
    private readonly IRulesManager<EnhancedTransformationRule> _rulesManager;
    private readonly ILogger<TransformationContext> _logger;
    private readonly TransformationStatistics _statistics = new TransformationStatistics();
    private readonly Stopwatch _stopwatch = new Stopwatch();
    private bool _isInitialized;

    /// <summary>
    /// Creates a new transformation context
    /// </summary>
    public TransformationContext(IRulesManager<EnhancedTransformationRule> rulesManager, ILogger<TransformationContext> logger)
    {
        _rulesManager = rulesManager ?? throw new ArgumentNullException(nameof(rulesManager));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Initializes the context with rules from a configuration
    /// </summary>
    public async Task InitializeAsync(EnhancedCleaningRules configuration)
    {
        if (configuration == null)
            throw new ArgumentNullException(nameof(configuration));

        Reset();

        var success = await _rulesManager.LoadRulesFromConfigAsync(configuration);

        if (!success)
        {
            throw new InvalidOperationException("Failed to initialize transformation context with rules");
        }

        _isInitialized = true;

        _logger.LogInformation("Initialized transformation context with {RuleCount} rules",
            _rulesManager.RuleCount);
    }

    /// <summary>
    /// Transforms a single record
    /// </summary>
    public Record TransformRecord(Record record)
    {
        if (!_isInitialized)
            throw new InvalidOperationException("Transformation context is not initialized");

        if (record == null)
            throw new ArgumentNullException(nameof(record));

        // Create a clone to avoid modifying the original
        var clonedRecord = record.Clone();

        _stopwatch.Start();

        try
        {
            // Apply rules
            _rulesManager.ApplyRules(clonedRecord);

            // Update statistics
            _statistics.RecordsProcessed++;

            // Estimate rules applied as the number of transformations in all columns
            int rulesApplied = 0;
            foreach (var column in clonedRecord.Columns)
            {
                rulesApplied += column.AppliedTransformations.Count;
            }
            _statistics.RulesApplied += rulesApplied;

            return clonedRecord;
        }
        finally
        {
            _stopwatch.Stop();
            _statistics.ProcessingTime = _stopwatch.Elapsed;
        }
    }

    /// <summary>
    /// Transforms a batch of records
    /// </summary>
    public RecordBatch TransformBatch(RecordBatch batch)
    {
        if (!_isInitialized)
            throw new InvalidOperationException("Transformation context is not initialized");

        if (batch == null)
            throw new ArgumentNullException(nameof(batch));

        // Create a new batch for the transformed records
        var resultBatch = new RecordBatch();

        _stopwatch.Start();

        try
        {
            // Apply rules to each record
            foreach (var record in batch.Records)
            {
                var transformedRecord = TransformRecord(record);
                resultBatch.Add(transformedRecord);
            }

            return resultBatch;
        }
        finally
        {
            _stopwatch.Stop();
            _statistics.ProcessingTime = _stopwatch.Elapsed;
        }
    }

    /// <summary>
    /// Gets statistics about the transformation process
    /// </summary>
    public TransformationStatistics GetStatistics()
    {
        return _statistics;
    }

    /// <summary>
    /// Resets the context
    /// </summary>
    public void Reset()
    {
        _rulesManager.ClearRules();
        _statistics.RecordsProcessed = 0;
        _statistics.RulesApplied = 0;
        _statistics.ProcessingTime = TimeSpan.Zero;
        _stopwatch.Reset();
        _isInitialized = false;
    }
}
