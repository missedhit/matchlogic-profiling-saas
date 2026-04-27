using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MatchLogic.Application.Interfaces.Core;

namespace MatchLogic.Infrastructure.Core.Telemetry;

public class RecordLinkageTelemetry : IDisposable, ITelemetry
{
    private readonly Meter _meter;
    private readonly Counter<long> _recordsProcessedCounter;
    private readonly Counter<long> _matchesFoundCounter;
    private readonly Histogram<double> _processingDurationHistogram;
    private readonly ILogger _logger;

    public RecordLinkageTelemetry(ILogger<RecordLinkageTelemetry> logger)
    {
        _logger = logger;
        _meter = new Meter("MatchLogic.RecordLinkage", "1.0.0");

        _recordsProcessedCounter = _meter.CreateCounter<long>(
            "records_processed",
            description: "Number of records processed");

        _matchesFoundCounter = _meter.CreateCounter<long>(
            "matches_found",
            description: "Number of matches found");

        _processingDurationHistogram = _meter.CreateHistogram<double>(
            "processing_duration_ms",
            unit: "ms",
            description: "Duration of processing operations");
    }

    public IDisposable MeasureOperation(string operationName)
    {
        return new OperationTimer(
            operationName,
            duration => _processingDurationHistogram.Record(
                duration,
                new KeyValuePair<String, object?>("operation", operationName)));
    }

    public void RecordProcessed(long count = 1)
    {
        _recordsProcessedCounter.Add(count);
    }

    public void MatchFound(long count = 1)
    {
        _matchesFoundCounter.Add(count);
    }

    public void Dispose()
    {
        _meter?.Dispose();
    }

    private class OperationTimer : IDisposable
    {
        private readonly string _operationName;
        private readonly Action<double> _recordDuration;
        private readonly Stopwatch _stopwatch;

        public OperationTimer(string operationName, Action<double> recordDuration)
        {
            _operationName = operationName;
            _recordDuration = recordDuration;
            _stopwatch = Stopwatch.StartNew();
        }

        public void Dispose()
        {
            _stopwatch.Stop();
            _recordDuration(_stopwatch.Elapsed.TotalMilliseconds);
        }
    }
}

