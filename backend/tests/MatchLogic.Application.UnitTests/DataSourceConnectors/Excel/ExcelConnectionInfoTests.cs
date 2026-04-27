using MatchLogic.Application.Features.Import;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPOI.XSSF.UserModel;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.Excel;

public class ExcelConnectionInfoTests
{
    private readonly ExcelConnectionConfig _sut;
    private const string ValidExcelPath = "test.xlsx";
    private const string InvalidExtensionPath = "test.txt";
    private const string NonExistentPath = "nonexistent.xlsx";

    private IConnectionBuilder _connectionBuilder;

    public ExcelConnectionInfoTests()
    {
        _sut = new ExcelConnectionConfig();
        string _dbPath = Path.GetTempFileName();
        string _dbJobPath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        IServiceCollection services = new ServiceCollection();
        services.AddLogging();
        services.AddApplicationSetup(_dbPath, _dbJobPath);
        var _serviceProvider = services.BuildServiceProvider();
        //var excelFunc = _serviceProvider.GetRequiredService<IDataReaderFactory>();
        _connectionBuilder = _serviceProvider.GetService<IConnectionBuilder>();
        //_sut.dataReaderFactory = excelFunc;
    }

    [Fact]
    public void Type_ShouldReturn_ExcelDataSourceType()
    {       
        // Act
        var result = _sut.CanCreateFromArgs(DataSourceType.Excel);
        // Assert
        Assert.True(result);
    }

    [Theory]
    [InlineData(".xlsx")]
    [InlineData(".xls")]
    public void ValidateConnection_WithValidExtension_ShouldReturnTrue(string extension)
    {
        // Arrange
        var filePath = $"test{extension}";
        File.WriteAllText(filePath, "dummy content");

        try
        {
            _sut.Parameters["FilePath"] = filePath;
            // Act
            var result = _sut.ValidateConnection();
            // Assert
            Assert.True(result);
          
        }
        finally
        {
            // Cleanup
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    [Fact]
    public void ValidateConnection_WithInvalidExtension_ShouldReturnFalse()
    {
        // Arrange
        var filePath = Path.GetTempFileName();
        //_sut.Parameters["FilePath"] = InvalidExtensionPath;
        File.WriteAllText(filePath, "dummy content");

        try
        {
            // Arrange
            _sut.Parameters["FilePath"] = filePath;
            // Act
            var result = _sut.ValidateConnection();
            // Assert
            Assert.False(result);
            
        }
        finally
        {
            // Cleanup
            if (File.Exists(InvalidExtensionPath))
                File.Delete(InvalidExtensionPath);
        }
    }
    [Fact]
    public void ValidateConnection_WithNonExistentFile_ShouldReturnFalse()
    {
        // Arrange
        
        _sut.Parameters["FilePath"] = NonExistentPath;
        // Act
        var result = _sut.ValidateConnection();
        // Assert
        Assert.False(result);        
    }
    [Fact]
    public void ValidateConnection_WithMissingFilePath_ShouldReturnFalse()
    {
        // Arrange
        _sut.Parameters.Clear();
        // Act
        var result = _sut.ValidateConnection();
        // Assert
        Assert.False(result);
    }
    [Fact]
    public async Task TestConnection_WithValidExcelFile_ShouldReturnTrue()
    {
        // Arrange
        CreateValidExcelFile(ValidExcelPath);
        _sut.Parameters["FilePath"] = ValidExcelPath;

        try
        {
            // Act
            var reader = _connectionBuilder.WithArgs(DataSourceType.Excel, new Dictionary<string, string>
            {
                { "FilePath", ValidExcelPath }
            }).Build();
            var result = await reader.TestConnectionAsync();
            //var result = await _sut.TestConnectionAsync();

            // Assert
            Assert.True(result);
        }
        finally
        {
            // Cleanup
            if (File.Exists(ValidExcelPath))
                File.Delete(ValidExcelPath);
        }
    }
    [Fact]
    public async Task TestConnection_WithInvalidFile_ShouldReturnFalse()
    {
        // Arrange
        File.WriteAllText(ValidExcelPath, "not a valid excel file");
        _sut.Parameters["FilePath"] = ValidExcelPath;

        try
        {
            // Act
            var reader = _connectionBuilder.WithArgs(DataSourceType.Excel, new Dictionary<string, string>
            {
                { "FilePath", ValidExcelPath }
            }).Build();
            var result = await reader.TestConnectionAsync();
            //var result = await _sut.TestConnectionAsync();

            // Assert
            Assert.False(result);
        }
        finally
        {
            // Cleanup
            if (File.Exists(ValidExcelPath))
                File.Delete(ValidExcelPath);
        }
    }

    [Fact]
    public async Task TestConnection_WithNonExistentFile_ShouldReturnFalse()
    {
        // Arrange
        _sut.Parameters["FilePath"] = NonExistentPath;

        // Act
        var reader = _connectionBuilder.WithArgs(DataSourceType.Excel, new Dictionary<string, string>
            {
                { "FilePath", NonExistentPath }
            }).Build();
        var result = await reader.TestConnectionAsync();
        //var result = await _sut.TestConnectionAsync();

        // Assert
        Assert.False(result);
    }

    /*[Fact]
    public void GetConnection_WithValidExcelFile_ShouldReturnExcelReader()
    {
        // Arrange
        CreateValidExcelFile(ValidExcelPath);
        _sut.Parameters["FilePath"] = ValidExcelPath;

        IExcelDataReader? reader = null;
        try
        {
            // Act

            reader = _sut.GetConnection() as IExcelDataReader;

            // Assert
            Assert.NotNull(reader);
            Assert.IsAssignableFrom<IExcelDataReader>(reader);
        }
        finally
        {
            // Cleanup
            reader?.Dispose();
            if (File.Exists(ValidExcelPath))
                File.Delete(ValidExcelPath);
        }
    }

    [Fact]
    public void GetConnection_WithMissingFilePath_ShouldThrowInvalidOperationException()
    {
        // Arrange
        _sut.Parameters.Clear();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(
            () => _sut.GetConnection());
        Assert.Equal("File path is required", exception.Message);
    }

    [Fact]
    public void GetConnection_WithNonExistentFile_ShouldThrowFileNotFoundException()
    {
        // Arrange
        _sut.Parameters["FilePath"] = NonExistentPath;

        // Act & Assert
        var exception = Assert.Throws<FileNotFoundException>(
            () => _sut.GetConnection());
        Assert.Equal("Excel file not found", exception.Message);
    }*/

    private void CreateValidExcelFile(string path)
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");
        var row = sheet.CreateRow(0);
        var cell = row.CreateCell(0);
        cell.SetCellValue("Test");

        using var fileStream = File.Create(path);
        workbook.Write(fileStream);
    }
}
