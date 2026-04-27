using MatchLogic.Application.Extensions;
using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Domain.Import;
using MatchLogic.Infrastructure.Persistence;
using LiteDB;
using Microsoft.Extensions.Logging;
using NPOI.XSSF.UserModel;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace MatchLogic.Application.UnitTests;
public class LiteDbDataStoreTest
{
    private readonly string _testExcelPath;
    private readonly string _dbFilePath;
    private readonly ILogger _logger;
    public LiteDbDataStoreTest()
    {
        _testExcelPath = Path.Combine(Path.GetTempPath(), "testNumberColumnNames.xlsx");
        //_excelFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "TestData", "SampleData1.xlsx");
        _dbFilePath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<LiteDbDataStoreTest>();
        CreateTestExcelFile();
    }

    private void CreateTestExcelFile()
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var sheet2 = workbook.CreateSheet("Sheet2");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("Name");
        headerRow.CreateCell(1).SetCellValue("_Name");
        headerRow.CreateCell(2).SetCellValue("Name_");
        headerRow.CreateCell(3).SetCellValue("2222");
        headerRow.CreateCell(4).SetCellValue("2Name");
        headerRow.CreateCell(5).SetCellValue("Name2");
        headerRow.CreateCell(6).SetCellValue("Full Name");
        headerRow.CreateCell(7).SetCellValue("Age");
        headerRow.CreateCell(8).SetCellValue("Zip");

        // Add test data
        var row1 = sheet.CreateRow(1);
        row1.CreateCell(0).SetCellValue("John Doe");
        row1.CreateCell(1).SetCellValue("Aryn Boyne");
        row1.CreateCell(2).SetCellValue("Mickey Pasley");
        row1.CreateCell(3).SetCellValue("Abagail Manuel");
        row1.CreateCell(4).SetCellValue("Colene McClean");
        row1.CreateCell(5).SetCellValue("Brett Pierce");
        row1.CreateCell(6).SetCellValue("Sofia Allner");
        row1.CreateCell(7).SetCellValue(30);
        row1.CreateCell(8).SetCellValue("84791");

        var row2 = sheet.CreateRow(2);
        row2.CreateCell(0).SetCellValue("Jane Smith");
        row2.CreateCell(1).SetCellValue("Kala Clifforth");
        row2.CreateCell(2).SetCellValue("Jud Billows");
        row2.CreateCell(3).SetCellValue("Worth Truder");
        row2.CreateCell(4).SetCellValue("Danell Huntingford");
        row2.CreateCell(5).SetCellValue("Byran Dumphries");
        row2.CreateCell(6).SetCellValue("Filippa Hadleigh");
        row2.CreateCell(7).SetCellValue(25);
        row2.CreateCell(8).SetCellValue("84792");

        using var fileStream = File.Create(_testExcelPath);
        workbook.Write(fileStream);
    }
    [Fact]
    public async Task ImportDataAsync_DigitColumnNamingSearch_Should_Success()
    {
        // Arrange
        var excelReader = new Infrastructure.Import.ExcelDataReader(new ExcelConnectionConfig() { FilePath = _testExcelPath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(excelReader, liteDbStore, _logger);

        // Act
        var jobId = await importModule.ImportDataAsync();

        excelReader.Dispose();
        var collectionName = GuidCollectionNameConverter.ToValidCollectionName(jobId);
        var result1 = await liteDbStore.GetPagedJobWithSortingAndFilteringDataAsync(collectionName, 1, 10, "John Doe");
        var result2 = await liteDbStore.GetPagedJobWithSortingAndFilteringDataAsync(collectionName, 1, 10, "Aryn Boyne");
        var result3 = await liteDbStore.GetPagedJobWithSortingAndFilteringDataAsync(collectionName, 1, 10, "Abagail Manuel");
        var result4 = await liteDbStore.GetPagedJobWithSortingAndFilteringDataAsync(collectionName, 1, 10, "Colene McClean");

        // Assert
        Assert.True(result1.TotalCount == 1);
        Assert.True(result2.TotalCount == 1);
        Assert.True(result3.TotalCount == 1);
        Assert.True(result4.TotalCount == 1);

        //Assert.Equal(result.Data.First().Keys, importedData[0]["Name"]);
        //Assert.Equal("John Doe", result.Data[0]["Name"]);
        //Assert.Equal("30", result.Data[0]["Age"].ToString());
        //Assert.Equal("New York", result.Data[0]["City"]);

        liteDbStore.Dispose();
    }


    [Fact]
    public async Task ImportDataAsync_DigitSearch_Should_Success()
    {
        // Arrange
        var excelReader = new Infrastructure.Import.ExcelDataReader(new ExcelConnectionConfig() { FilePath = _testExcelPath }, _logger);
        var liteDbStore = new LiteDbDataStore(_dbFilePath, _logger);
        var importModule = new DataImportModule(excelReader, liteDbStore, _logger);

        // Act
        var jobId = await importModule.ImportDataAsync();

        excelReader.Dispose();
        var collectionName = GuidCollectionNameConverter.ToValidCollectionName(jobId);
        var result1 = await liteDbStore.GetPagedJobWithSortingAndFilteringDataAsync(collectionName, 1, 10, "8479");

        // Assert
        Assert.Equal(2, result1.TotalCount);

        liteDbStore.Dispose();
    }
}
