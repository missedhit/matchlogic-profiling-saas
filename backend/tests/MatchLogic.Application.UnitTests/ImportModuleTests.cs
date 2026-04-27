using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.Import;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Import;
using MatchLogic.Infrastructure.Persistence;
using LiteDB;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Application.UnitTests;

public class ImportModuleTests : IDisposable
{
    private readonly string _csvFilePath;
    private readonly string _excelFilePath;
    private readonly string _dbFilePath;
    private readonly ILogger _logger;
    public ImportModuleTests()
    {
        _csvFilePath = Path.GetTempFileName();
        _excelFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "SampleData1.xlsx");
        _dbFilePath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<ImportModuleTests>();

        // Create a sample CSV file
        File.WriteAllText(_csvFilePath,
            "Name,Age,City\n" +
            "John Doe,30,New York\n" +
            "Jane Smith,25,Los Angeles\n" +
            "Bob Johnson,45,Chicago");

    }

    [Fact]
    public async Task ImportDataAsync_ShouldImportCsvDataToLiteDb()
    {
        // Arrange
        var csvReader = new CsvDataReaderOptimized(new CSVConnectionConfig() { FilePath = _csvFilePath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(csvReader, liteDbStore, _logger);

        // Act
        var jobId = await importModule.ImportDataAsync();

        liteDbStore.Dispose();
        csvReader.Dispose();

        // Assert
        using (var db = new LiteDatabase(_dbFilePath))
        {
            var collection = db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId));
            var importedData = collection.FindAll().ToList();

            Assert.Equal(3, importedData.Count);
            Assert.Equal("John Doe", importedData[0]["Name"].AsString);
            Assert.Equal(30, importedData[0]["Age"].AsInt32);
            Assert.Equal("New York", importedData[0]["City"].AsString);
        }
    }

    [Fact]
    public async Task ImportDataAsync_ShouldImportExcelDataToLiteDb()
    {
        // Arrange
        var excelReader = new MatchLogic.Infrastructure.Import.ExcelDataReader(new ExcelConnectionConfig() { FilePath = _excelFilePath }, _logger);

        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(excelReader, liteDbStore, _logger);

        // Act
        var jobId = await importModule.ImportDataAsync();

        excelReader.Dispose();
        liteDbStore.Dispose();

        // Assert
        using (var db = new LiteDatabase(_dbFilePath))
        {
            var collection = db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId));
            var importedData = collection.FindAll().ToList();

            Assert.Equal(3, importedData.Count);
            Assert.Equal("John Doe", importedData[0]["Name"].AsString);
            Assert.Equal(30, importedData[0]["Age"].AsInt32);
            Assert.Equal("New York", importedData[0]["City"].AsString);
        }
    }

    [Fact]
    public async Task ImportDataAsync_ShouldReadUsingIDataStoreReadFunction()
    {
        // Arrange
        var csvReader = new CsvDataReaderOptimized(new CSVConnectionConfig() { FilePath = _csvFilePath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(csvReader, liteDbStore, _logger);

        // Act
        var jobId = await importModule.ImportDataAsync();

        csvReader.Dispose();

        var importedData = (await liteDbStore.GetJobDataAsync(jobId)).ToList();

        // Assert
        Assert.Equal(3, importedData.Count);
        Assert.Equal("John Doe", importedData[0]["Name"]);
        Assert.Equal("30", importedData[0]["Age"]);
        Assert.Equal("New York", importedData[0]["City"]);

        liteDbStore.Dispose();

    }

    [Fact]
    public async Task ImportDataAsync_ShouldReadExcelUsingIDataStoreReadFunction()
    {
        // Arrange
        var excelReader = new MatchLogic.Infrastructure.Import.ExcelDataReader(new ExcelConnectionConfig() { FilePath = _excelFilePath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(excelReader, liteDbStore, _logger);

        // Act
        var jobId = await importModule.ImportDataAsync();

        excelReader.Dispose();

        var importedData = (await liteDbStore.GetJobDataAsync(jobId)).ToList();

        // Assert
        Assert.Equal(3, importedData.Count);
        Assert.Equal("John Doe", importedData[0]["Name"]);
        Assert.Equal("30", importedData[0]["Age"].ToString());
        Assert.Equal("New York", importedData[0]["City"]);

        liteDbStore.Dispose();
    }

    [Fact]
    public async Task ImportDataAsync_ShouldHandleLargeDataSet()
    {
        // Arrange
        const int recordCount = 10000;
        CreateLargeCsvFile(_csvFilePath, recordCount);

        var csvReader = new CsvDataReaderOptimized(new CSVConnectionConfig() { FilePath = _csvFilePath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(csvReader, liteDbStore, _logger);

        // Act
        var jobId = await importModule.ImportDataAsync();

        csvReader.Dispose();
        liteDbStore.Dispose();

        // Assert
        using (var db = new LiteDatabase(_dbFilePath))
        {
            var collection = db.GetCollection<BsonDocument>(GuidCollectionNameConverter.ToValidCollectionName(jobId));
            var importedData = collection.FindAll().ToList();

            Assert.Equal(recordCount, importedData.Count);
        }
    }

    /*[Fact]
    public async Task ImportDataAsync_ShouldHandleCancellation()
    {
        // Arrange
        const int recordCount = 10000;
        CreateLargeCsvFile(_csvFilePath, recordCount);

        var csvReader = new CsvDataReader(_csvFilePath, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(csvReader, liteDbStore, _logger);

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(60)); // Cancel after 50ms

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            async () => await importModule.ImportDataAsync(cts.Token)
        );

        csvReader.Dispose();
        liteDbStore.Dispose();
    }*/

    private void CreateLargeCsvFile(string filePath, int recordCount)
    {
        using (var writer = new StreamWriter(filePath))
        {
            writer.WriteLine("Name,Age,City");
            for (int i = 0; i < recordCount; i++)
            {
                int cityIndex = (i % 3);
                string city = string.Empty;
                switch (cityIndex)
                {
                    case 0:
                        city = "New York";
                        break;
                    case 1:
                        city = "Los Angeles";
                        break;
                    case 2:
                        city = "Chicago";
                        break;
                }
                writer.WriteLine($"Person{i},{i % 100},{city}");
            }
        }
    }

    public void Dispose()
    {
        File.Delete(_csvFilePath);
        File.Delete(_dbFilePath);
    }
}
