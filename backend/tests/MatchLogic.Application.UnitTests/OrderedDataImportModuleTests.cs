using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Common;
using MatchLogic.Application.Interfaces.Events;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Entities.Common;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.BackgroundJob;
using MatchLogic.Infrastructure.Common;
using MatchLogic.Infrastructure.Import;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class OrderedDataImportModuleTests : IDisposable
{
    private readonly string _csvFilePath;
    private readonly string _dbFilePath;
    private readonly ILogger<CsvDataReaderOptimized> _logger;
    private readonly IRecordHasher _recordHasher;

    public OrderedDataImportModuleTests()
    {
        _csvFilePath = Path.GetTempFileName();
        _dbFilePath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        //_logger = loggerFactory.CreateLogger<OrderedDataImportModuleTests>();
        _logger = loggerFactory.CreateLogger<CsvDataReaderOptimized>();
        _recordHasher = new SHA256RecordHasher();

        File.WriteAllText(_csvFilePath,
            "Name,Age,City\n" +
            "John Doe,30,New York\n" +
            "Jane Smith,25,Los Angeles\n" +
            "Bob Johnson,45,Chicago");
    }

    [Fact]
    public async Task ImportDataAsync_ShouldPreserveSourceOrder()
    {
        // Arrange
        var csvReader = new CsvDataReaderOptimized(new CSVConnectionConfig() { FilePath = _csvFilePath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var jobEventPublisher = new Mock<IJobEventPublisher>();
        var context = new Mock<ICommandContext>();
        jobEventPublisher.Setup(x => x.CreateStepTracker(It.IsAny<Guid>(),
           It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>()))
           .Returns(new Mock<IStepProgressTracker>().Object);
        context.Setup(x => x.Statistics).Returns(new Domain.CleansingAndStandaradization.FlowStatistics());
        var importModule = new OrderedDataImportModule(csvReader, liteDbStore, _logger, _recordHasher, jobEventPublisher.Object, context.Object);

        // Act
        var jobId = await importModule.ImportDataAsync();
        var importedData = (await liteDbStore.GetJobDataAsync(jobId)).ToList();

        liteDbStore.Dispose();
        csvReader.Dispose();

        // Assert
        Assert.Equal(3, importedData.Count());
        for (int i = 0; i < importedData.Count(); i++)
        {
            var metadata = JsonSerializer.Deserialize<RecordMetadata>(
                JsonSerializer.Serialize(importedData[i]["_metadata"]));
            Assert.Equal(i, metadata?.RowNumber);
        }

        Assert.Equal("John Doe", importedData[0]["Name"]);
        Assert.Equal("Jane Smith", importedData[1]["Name"]);
        Assert.Equal("Bob Johnson", importedData[2]["Name"]);
    }

    [Fact]
    public async Task ImportDataAsync_ShouldIncludeMetadata()
    {
        // Arrange
        var csvReader = new CsvDataReaderOptimized(new CSVConnectionConfig() { FilePath = _csvFilePath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var jobEventPublisher = new Mock<IJobEventPublisher>();
        jobEventPublisher.Setup(x => x.CreateStepTracker(It.IsAny<Guid>(),
           It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>()))
           .Returns(new Mock<IStepProgressTracker>().Object);
        var context = new Mock<ICommandContext>();
        context.Setup(x => x.Statistics).Returns(new Domain.CleansingAndStandaradization.FlowStatistics());
        var importModule = new OrderedDataImportModule(csvReader, liteDbStore, _logger, _recordHasher, jobEventPublisher.Object, context.Object);

        // Act
        var jobId = await importModule.ImportDataAsync();
        var importedData = (await liteDbStore.GetJobDataAsync(jobId)).ToList();

        liteDbStore.Dispose();
        csvReader.Dispose();

        // Assert
        Assert.All(importedData, record =>
        {
            var metadata = JsonSerializer.Deserialize<RecordMetadata>(
                JsonSerializer.Serialize(record["_metadata"]));
            Assert.NotNull(metadata.Hash);
            Assert.Equal(Path.GetFileName(_csvFilePath), metadata.SourceFile);
            Assert.True(metadata.RowNumber > -1);
        });
    }

    [Fact]
    public async Task ImportDataAsync_ShouldHandleLargeDataSet()
    {
        // Arrange
        const int recordCount = 10000;
        CreateLargeCsvFile(_csvFilePath, recordCount);
        var csvReader = new CsvDataReaderOptimized(new CSVConnectionConfig() { FilePath = _csvFilePath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var jobEventPublisher = new Mock<IJobEventPublisher>();
        jobEventPublisher.Setup(x => x.CreateStepTracker(It.IsAny<Guid>(),
            It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>()))
            .Returns(new Mock<IStepProgressTracker>().Object);
        var context = new Mock<ICommandContext>();
        context.Setup(x => x.Statistics).Returns(new Domain.CleansingAndStandaradization.FlowStatistics());

        var importModule = new OrderedDataImportModule(csvReader, liteDbStore, _logger, _recordHasher, jobEventPublisher.Object, context.Object);

        // Act
        var jobId = await importModule.ImportDataAsync();
        var importedData = (await liteDbStore.GetJobDataAsync(jobId)).ToList();

        liteDbStore.Dispose();
        csvReader.Dispose();

        // Assert
        Assert.Equal(recordCount, importedData.Count);

        // Verify order is preserved
        for (int i = 0; i < importedData.Count; i++)
        {
            var metadata = JsonSerializer.Deserialize<RecordMetadata>(
                JsonSerializer.Serialize(importedData[i]["_metadata"]));
            Assert.Equal(i, metadata?.RowNumber);
        }
    }

    private void CreateLargeCsvFile(string filePath, int recordCount)
    {
        using var writer = new StreamWriter(filePath);
        writer.WriteLine("Name,Age,City");
        for (int i = 0; i < recordCount; i++)
        {
            string city = (i % 3) switch
            {
                0 => "New York",
                1 => "Los Angeles",
                _ => "Chicago"
            };
            writer.WriteLine($"Person{i},{i % 100},{city}");
        }
    }

    public void Dispose()
    {
        File.Delete(_csvFilePath);
        File.Delete(_dbFilePath);
    }
}
