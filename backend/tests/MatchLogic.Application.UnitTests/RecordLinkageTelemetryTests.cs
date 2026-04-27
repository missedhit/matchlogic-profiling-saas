using MatchLogic.Infrastructure.Core.Telemetry;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class RecordLinkageTelemetryTests
{
    private readonly Mock<ILogger<RecordLinkageTelemetry>> _logger;
    private readonly MeterListener _meterListener;
    private readonly ConcurrentDictionary<string, long> _measurements;
    private readonly ConcurrentBag<(string Name, double Value)> _histogramMeasurements;

    public RecordLinkageTelemetryTests()
    {
        _logger = new Mock<ILogger<RecordLinkageTelemetry>>();
        _measurements = new ConcurrentDictionary<string, long>();
        _histogramMeasurements = new ConcurrentBag<(string Name, double Value)>();

        _meterListener = new MeterListener();
        _meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == "MatchLogic.RecordLinkage")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };

        _meterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
        {
            _measurements.AddOrUpdate(
                instrument.Name,
                measurement,
                (_, current) => current + measurement);
        });

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
            _histogramMeasurements.Add((operationName, measurement));
        });

        _meterListener.Start();
    }

    [Fact]
    public void Constructor_InitializesMetricsCorrectly()
    {
        // Act
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);

        // Assert
        Assert.NotNull(telemetry);
    }

    [Fact]
    public void RecordProcessed_SingleRecord_IncrementsCounter()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);

        // Act
        telemetry.RecordProcessed();

        // Assert
        Assert.True(_measurements.TryGetValue("records_processed", out var count));
        Assert.Equal(1, count);
    }

    [Fact]
    public void RecordProcessed_MultipleRecords_IncrementsCounterBySpecifiedAmount()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);

        // Act
        telemetry.RecordProcessed(5);

        // Assert
        Assert.True(_measurements.TryGetValue("records_processed", out var count));
        Assert.Equal(5, count);
    }

    [Fact]
    public void MatchFound_SingleMatch_IncrementsCounter()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);

        // Act
        telemetry.MatchFound();

        // Assert
        Assert.True(_measurements.TryGetValue("matches_found", out var count));
        Assert.Equal(1, count);
    }

    [Fact]
    public void MatchFound_MultipleMatches_IncrementsCounterBySpecifiedAmount()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);

        // Act
        telemetry.MatchFound(3);

        // Assert
        Assert.True(_measurements.TryGetValue("matches_found", out var count));
        Assert.Equal(3, count);
    }

    [Fact]
    public void MeasureOperation_RecordsDuration()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);
        var operationName = "test_operation";

        // Act
        using (telemetry.MeasureOperation(operationName))
        {
            Thread.Sleep(100); // Simulate work
        }

        // Assert
        var measurements = _histogramMeasurements
            .Where(m => m.Name == operationName)
            .Select(m => m.Value)
            .ToList();

        Assert.Single(measurements);
        Assert.True(measurements[0] >= 100); // At least 100ms
    }

    [Fact]
    public void MeasureOperation_MultipleOperations_RecordsEachSeparately()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);
        var operation1 = "operation1";
        var operation2 = "operation2";

        // Act
        using (telemetry.MeasureOperation(operation1))
        {
            Thread.Sleep(50);
        }

        using (telemetry.MeasureOperation(operation2))
        {
            Thread.Sleep(100);
        }

        // Assert
        var measurements = _histogramMeasurements
            .GroupBy(m => m.Name)
            .ToDictionary(g => g.Key, g => g.Select(m => m.Value).First());

        Assert.Equal(2, measurements.Count);
        Assert.True(measurements[operation1] >= 50);
        Assert.True(measurements[operation2] >= 100);
    }

    [Fact]
    public void MeasureOperation_NestedOperations_RecordsCorrectly()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);
        var outerOperation = "outer";
        var innerOperation = "inner";

        // Act
        using (telemetry.MeasureOperation(outerOperation))
        {
            Thread.Sleep(50);
            using (telemetry.MeasureOperation(innerOperation))
            {
                Thread.Sleep(100);
            }
            Thread.Sleep(50);
        }

        // Assert
        var measurements = _histogramMeasurements
            .GroupBy(m => m.Name)
            .ToDictionary(g => g.Key, g => g.Select(m => m.Value).First());

        Assert.Equal(2, measurements.Count);
        Assert.True(measurements[innerOperation] >= 100);
        Assert.True(measurements[outerOperation] >= 200);
    }

    [Fact]
    public void MixedOperations_RecordsAllMetricsCorrectly()
    {
        // Arrange
        using var telemetry = new RecordLinkageTelemetry(_logger.Object);

        // Act
        using (telemetry.MeasureOperation("mixed_test"))
        {
            telemetry.RecordProcessed(5);
            Thread.Sleep(50);
            telemetry.MatchFound(2);
            Thread.Sleep(50);
            telemetry.RecordProcessed(3);
        }

        // Assert
        Assert.True(_measurements.TryGetValue("records_processed", out var processedCount));
        Assert.Equal(8, processedCount);

        Assert.True(_measurements.TryGetValue("matches_found", out var matchesCount));
        Assert.Equal(2, matchesCount);

        var durationMeasurements = _histogramMeasurements
            .Where(m => m.Name == "mixed_test")
            .Select(m => m.Value)
            .ToList();

        Assert.Single(durationMeasurements);
        Assert.True(durationMeasurements[0] >= 100);
    }

    [Fact]
    public void Dispose_CleanupsMeter()
    {
        // Arrange
        var telemetry = new RecordLinkageTelemetry(_logger.Object);

        // Act
        telemetry.Dispose();

        // Attempting to use after disposal
        telemetry.RecordProcessed();

        // Assert
        // The measurement won't be recorded after disposal
        Assert.False(_measurements.TryGetValue("records_processed", out _));
    }
}
