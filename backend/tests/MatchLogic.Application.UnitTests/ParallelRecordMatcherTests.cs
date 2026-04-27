using CsvHelper;
using MatchLogic.Application.Features.DataMatching;
using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Core;
using MatchLogic.Application.Interfaces.DataMatching;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Core.Telemetry;
using MatchLogic.Infrastructure.Import;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class ParallelRecordMatcherTests
{
    private readonly IBlockingStrategy _blockingStrategy;
    private readonly IQGramIndexer _qGramIndexer;
    private readonly Mock<ILogger<OptimizedQGramIndexer>> _logger;
    private readonly Mock<ILogger> _genericlogger;
    private readonly RecordLinkageOptions _options;
    private readonly Mock<ITelemetry> _mockTelemetry;
    private ParallelRecordMatcher _matcher;
    private SimpleRecordPairer _simpleRecordPairer;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker;
    private readonly Mock<IStepProgressTracker> _mockProgressTracker1;

    public ParallelRecordMatcherTests()
    {
        _logger = new Mock<ILogger<OptimizedQGramIndexer>>();
        _genericlogger = new Mock<ILogger>();
        _mockProgressTracker = new Mock<IStepProgressTracker>();
        _mockProgressTracker1 = new Mock<IStepProgressTracker>();
        _qGramIndexer = new OptimizedQGramIndexer(q: 3, _logger.Object);
        _options = new RecordLinkageOptions();
        _mockTelemetry = new Mock<ITelemetry>();
        var optionsWrapper = new OptionsWrapper<RecordLinkageOptions>(_options);
        _simpleRecordPairer = new SimpleRecordPairer(new Mock<ILogger<SimpleRecordPairer>>().Object, optionsWrapper);
        _blockingStrategy = new ParallelBlockingStrategy(new Mock<ILogger<ParallelBlockingStrategy>>().Object, optionsWrapper, _mockTelemetry.Object);
        _matcher = new ParallelRecordMatcher(
            _blockingStrategy,
            _qGramIndexer,
            _simpleRecordPairer,
            new Mock<ILogger<ParallelRecordMatcher>>().Object,
            optionsWrapper,
            _mockTelemetry.Object);
    }

    [Fact]
    public async Task FindMatchesAsync_WithExactCriteria_FindsMatches()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object>
            {
                { "id", 1 },
                { "firstName", "John" },
                { "lastName", "Smith" },
                { "city", "New York" }
            },
            new Dictionary<string, object>
            {
                { "id", 2 },
                { "firstName", "John" },
                { "lastName", "Smith" },
                { "city", "New York" }
            },
            new Dictionary<string, object>
            {
                { "id", 3 },
                { "firstName", "Jane" },
                { "lastName", "Doe" },
                { "city", "Boston" }
            }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "firstName",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.95" }
                }
            },
            new MatchCriteria
            {
                FieldName = "lastName",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Single(matches);
        var (record1, record2) = matches[0];
        Assert.Equal("John", record1["firstName"]);
        Assert.Equal("Smith", record1["lastName"]);
        Assert.Equal("John", record2["firstName"]);
        Assert.Equal("Smith", record2["lastName"]);
    }

    [Fact]
    public async Task FindMatchesAsync_WithFuzzyCriteria_FindsSimilarMatches()
    {
        // Arrange
        var records = new[]
        {
            new Dictionary<string, object>
            {
                { "id", 1 },
                { "name", "John Smith" },
                { "address", "123 Main Street" }
            },
            new Dictionary<string, object>
            {
                { "id", 2 },
                { "name", "Jon Smithh" },
                { "address", "123 Main St" }
            },
            new Dictionary<string, object>
            {
                { "id", 3 },
                { "name", "Jane Doe" },
                { "address", "456 Oak Avenue" }
            }
        }.ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string>
                {
                    { ArgsValue.FastLevel, "0.3" }
                }
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Single(matches);
        var (record1, record2) = matches[0];
        Assert.Equal("John Smith", record1["name"]);
        Assert.Equal("Jon Smithh", record2["name"]);
    }

    [Fact]
    public async Task FindMatchesAsync_WithLargeBatch_ProcessesCorrectly()
    {
        // Arrange
        var recordCount = _options.BatchSize * 2 + 10;
        var records = Enumerable.Range(0, recordCount)
            .Select(i => new Dictionary<string, object>
            {
                { "id", i },
                { "name", $"Person {i}" },
                { "code", i % 2 == 0 ? "A123" : "B456" }
            })
            .ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "code",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text,
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.NotEmpty(matches);
        foreach (var (record1, record2) in matches)
        {
            Assert.Equal(record1["code"], record2["code"]);
        }
    }

    [Fact]
    public async Task FindMatchesAsync_WithEmptyRecords_ReturnsEmptyResult()
    {
        // Arrange
        var records = AsyncEnumerable.Empty<IDictionary<string, object>>();
        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "name",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.Empty(matches);
    }

    [Fact]
    public async Task FindMatchesAsync_WhenDisposed_ThrowsObjectDisposedException()
    {
        // Arrange
        await _matcher.DisposeAsync();
        var records = AsyncEnumerable.Empty<IDictionary<string, object>>();
        var criteria = Array.Empty<MatchCriteria>();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
            await result.ToListAsync();
        });
    }

    [Fact]
    public async Task FindMatchesAsync_WithMultipleThreads_HandlesProcessingCorrectly()
    {
        // Arrange
        var records = Enumerable.Range(0, 500)
            .Select(i => new Dictionary<string, object>
            {
                { "id", i },
                { "group", $"Group{i%5}" },
                { "value", $"Value{i}" }
            })
            .ToAsyncEnumerable();

        var criteria = new[]
        {
            new MatchCriteria
            {
                FieldName = "group",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary<ArgsValue, string> { { ArgsValue.FastLevel, "0.7" } }
            }
        };

        // Act
        var result = await _matcher.FindMatchesAsync(records, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);
        var matches = await result.ToListAsync();

        // Assert
        Assert.NotEmpty(matches);
        foreach (var (record1, record2) in matches)
        {
            Assert.StartsWith("Group", record1["group"].ToString());
            Assert.StartsWith("Group", record2["group"].ToString());
        }
    }

    // NOTE: end-to-end test is disabled by default
    //[Fact]
    public async Task End_toEnd_Async()
    {
        var _excelFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Example Data 1.xlsx");
        var _dbFilePath = Path.GetTempFileName();

        var excelReader = new MatchLogic.Infrastructure.Import.ExcelDataReader(new ExcelConnectionConfig() { FilePath = _excelFilePath }, _logger.Object);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger.Object);
        var importModule = new DataImportModule(excelReader, liteDbStore, _logger.Object);

        // Act
        var jobId = await importModule.ImportDataAsync();

        excelReader.Dispose();

        var criteria = new[]
       {
            new MatchCriteria
            {
                FieldName = "City",
                MatchingType = MatchingType.Exact,
                DataType = CriteriaDataType.Text,
            },
            new MatchCriteria
            {
                FieldName = "Company Name",
                MatchingType = MatchingType.Fuzzy,
                DataType = CriteriaDataType.Text,
                Arguments = new Dictionary < ArgsValue, string > { { ArgsValue.FastLevel, "0.7" } }
            }
        };

        var data = liteDbStore.StreamJobDataAsync(jobId, _mockProgressTracker.Object);
        var result = _matcher.FindMatchesAsync(data, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);

        //Todo: check this method signature
        //await AsyncExcelWriter.WriteToExcelWithBatchingAsync(result, "output-result1.csv", _options.BatchSize);

        //Assert.NotEmpty(matches);
    }

    // NOTE: end-to-end test is disabled by default
    // [Fact]
    public async Task End_toEnd__2M_Async()
    {
        Dictionary<string, long> _counterMeasurements = new Dictionary<string, long>();
        List<(string Name, double Value)> _histogramMeasurements = new List<(string Name, double Value)>();

        MeterListener _meterListener = new MeterListener();
        _meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "MatchLogic.RecordLinkage")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };

        // Listen for counter measurements
        _meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
        {
            lock (_counterMeasurements)
            {
                if (!_counterMeasurements.ContainsKey(instrument.Name))
                    _counterMeasurements[instrument.Name] = 0;
                _counterMeasurements[instrument.Name] += measurement;
            }
        });

        // Listen for histogram measurements
        _meterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
        {
            var operationName = string.Empty;
            if (tags != null)
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "operation")
                    {
                        operationName = tag.Value?.ToString() ?? string.Empty;
                        break;
                    }
                }
            }
            lock (_histogramMeasurements)
            {
                _histogramMeasurements.Add((operationName, measurement));
            }
        });

        _meterListener.Start();
        //_telemetry = new RecordLinkageTelemetry(_logger);
        var _excelFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "Companies2M.csv");
        var _dbFilePath = Path.GetTempFileName();
        var csvReader = new CsvDataReaderOptimized(new CSVConnectionConfig() { FilePath = _excelFilePath }, _genericlogger.Object);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger.Object);
        var importModule = new DataImportModule(csvReader, liteDbStore, _logger.Object);

        // Act
        var jobId = await importModule.ImportDataAsync();

        csvReader.Dispose();

        var criteria = new[]
       {
             new MatchCriteria
             {
                 FieldName = "City",
                 MatchingType = MatchingType.Exact,
                 DataType = CriteriaDataType.Text,
             },
              new MatchCriteria
             {
                 FieldName = "State",
                 MatchingType = MatchingType.Exact,
                 DataType = CriteriaDataType.Text,
             },
             new MatchCriteria
             {
                 FieldName = "CompanyName",
                 MatchingType = MatchingType.Fuzzy,
                 DataType = CriteriaDataType.Text,
                 Arguments = new Dictionary < ArgsValue, string > { { ArgsValue.FastLevel, "0.7" } }
             }
         };
        //  var criteria = new[]
        //{
        //      new MatchCriteria
        //      {
        //          FieldName = "City",
        //          MatchingType = MatchingType.Exact,
        //          DataType = CriteriaDataType.Text,
        //      },
        //      new MatchCriteria
        //      {
        //          FieldName = "Company Name",
        //          MatchingType = MatchingType.Fuzzy,
        //          DataType = CriteriaDataType.Text,
        //          Arguments = new Dictionary < ArgsValue, string > { { ArgsValue.FastLevel, "0.7" } }
        //      }
        //  };
        var data = liteDbStore.StreamJobDataAsync(jobId, _mockProgressTracker.Object);
        var telementry = new RecordLinkageTelemetry(new Mock<ILogger<RecordLinkageTelemetry>>().Object);
        var optionsWrapper = new OptionsWrapper<RecordLinkageOptions>(_options);
        var blockingStrategy = new ParallelBlockingStrategy(new Mock<ILogger<ParallelBlockingStrategy>>().Object, optionsWrapper, _mockTelemetry.Object);
        var matcher = new ParallelRecordMatcher(blockingStrategy, _qGramIndexer, _simpleRecordPairer, new Mock<ILogger<ParallelRecordMatcher>>().Object, optionsWrapper, telementry);
        var result = await matcher.FindMatchesAsync(data, criteria, _mockProgressTracker.Object, _mockProgressTracker1.Object);

        //await AsyncExcelWriter.WriteToExcelWithBatchingAsync(result, "output-result2.csv", _options.BatchSize);

        var matches = await result.ToListAsync();

        // Wait a bit to ensure all telemetry is recorded
        await Task.Delay(100);

        Assert.True(_counterMeasurements.ContainsKey("records_processed"));
        Assert.True(_counterMeasurements.ContainsKey("matches_found"));
        Assert.NotEmpty(_histogramMeasurements);

        // Verify processing operations were measured
        Assert.Contains(_histogramMeasurements, m => m.Name == "find_matches");
        if (matches.Any())
        {
            Assert.True(_counterMeasurements["matches_found"] > 0);
        }

    }
    // Helper class for writing results
    public class ResultRecord
    {
        public string Record1 { get; set; }
        public string Record2 { get; set; }
    }

    public static class AsyncExcelWriter
    {
        public static async Task WriteToExcelWithBatchingAsync(
            IAsyncEnumerable<(IDictionary<string, object>, IDictionary<string, object>)> dataStream,
            string filePath,
            int batchSize = 1000)
        {
            using var writer = new StreamWriter(filePath);
            using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

            // Write headers
            csv.WriteHeader<ResultRecord>();
            await csv.NextRecordAsync();

            var batch = new List<(IDictionary<string, object>, IDictionary<string, object>)>(batchSize);

            await foreach (var item in dataStream)
            {
                batch.Add(item);

                if (batch.Count >= batchSize)
                {
                    await WriteBatchAsync(csv, batch);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await WriteBatchAsync(csv, batch);
            }
        }

        private static async Task WriteBatchAsync(
            CsvWriter csv,
            List<(IDictionary<string, object>, IDictionary<string, object>)> batch)
        {
            foreach (var (record1, record2) in batch)
            {
                csv.WriteField(string.Join(", ", record1.Select(kv => $"{kv.Key}:{kv.Value}")));
                csv.WriteField(string.Join(", ", record2.Select(kv => $"{kv.Key}:{kv.Value}")));
                await csv.NextRecordAsync();
            }
            await csv.FlushAsync();
        }
    }
}

