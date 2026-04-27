using MatchLogic.Application.Features.Import;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Import;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.CSV;

public class CSVConnectionInfoTests : IDisposable
{
    private CSVConnectionConfig _sut;
    private const string ValidCsvPath = "test.csv";
    private const string diskValidCsvPath = "C:\\ProgramData\\MatchLogicApi\\Uploads\\149c9ced-5c10-4682-a895-d929acdce48e.csv";
    private const string InvalidExtensionPath = "test.txt";
    private const string NonExistentPath = "nonexistent.csv";

    private readonly ILogger<CsvDataReaderOptimized> _logger;
    private IConnectionBuilder connectionBuilder;


    public CSVConnectionInfoTests()
    {
        _sut = new CSVConnectionConfig();


        // Setup any dependencies or mocks if needed
        // For example, if CSVConnectionInfo has dependencies on other services, mock them here
        string _dbPath = Path.GetTempFileName();
        string _dbJobPath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<CsvDataReaderOptimized>();
        // Create service collection
        IServiceCollection services = new ServiceCollection();
        services.AddLogging();
        services.AddApplicationSetup(_dbPath, _dbJobPath);
        var _serviceProvider = services.BuildServiceProvider();
        connectionBuilder = _serviceProvider.GetRequiredService<IConnectionBuilder>();        
    }


    [Fact]
    public void Type_ShouldReturn_CSVDataSourceType()
    {
        // Arrange
        // Act
        var result = _sut.CanCreateFromArgs(DataSourceType.CSV);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void ValidateConnection_WithValidCsvFile_ShouldReturnTrue()
    {
        _sut.Parameters["FilePath"] = ValidCsvPath;
        File.WriteAllText(ValidCsvPath, "col1,col2\nval1,val2");

        try
        {
            var result = _sut.ValidateConnection();
            Assert.True(result);
        }
        finally
        {
            if (File.Exists(ValidCsvPath))
                File.Delete(ValidCsvPath);
        }
    }

    [Fact]
    public void ValidateConnection_WithInvalidExtension_ShouldReturnFalse()
    {
        var tempFilePath = Path.GetTempFileName();
        File.WriteAllText(tempFilePath, "col1,col2\nval1,val2");

        try
        {

            _sut.Parameters["FilePath"] = tempFilePath;
            // Act            
            var result = _sut.ValidateConnection();
            // Assert
            Assert.False(result);
        }
        finally
        {
            if (File.Exists(tempFilePath))
                File.Delete(tempFilePath);
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
        _sut.Parameters.Clear();
        var result = _sut.ValidateConnection();
        Assert.False(result);
    }

    [Fact]
    public async Task TestConnection_WithValidCsvFile_ShouldReturnTrue()
    {
        // Create a new CSV file and write content so other processes can open it easily
        File.WriteAllText(ValidCsvPath, "col1,col2\nval1,val2");

        try
        {
            var parameters = new Dictionary<string, string>
            {
                { "FilePath", ValidCsvPath }
            };

            var reader = connectionBuilder.WithArgs(DataSourceType.CSV, parameters).Build();            
            var result = await reader.TestConnectionAsync();
            Assert.True(result);
        }
        finally
        {
            if (File.Exists(ValidCsvPath))
                File.Delete(ValidCsvPath);
        }
    }

    [Fact]
    public async Task TestConnection_WithNonExistentFile_ShouldReturnFalse()
    {
        //_sut.Parameters["FilePath"] = NonExistentPath;
        
        var reader = connectionBuilder.WithArgs(DataSourceType.CSV, new Dictionary<string, string>
            {
                { "FilePath", NonExistentPath }
            }).Build();
        var result = await reader.TestConnectionAsync();
        Assert.False(result);
    }

    [Fact]
    public async Task TestConnection_WithUnreadableFile_ShouldReturnFalse()
    {
        _sut.Parameters["FilePath"] = ValidCsvPath;
        // Create file and open it exclusively to simulate lock/unreadable
        using (var fs = new FileStream(ValidCsvPath, FileMode.Create, FileAccess.ReadWrite, FileShare.None))
        {
            var reader = connectionBuilder.WithArgs(DataSourceType.CSV, new Dictionary<string, string>
            {
                { "FilePath", NonExistentPath }
            }).Build();
            var result = await reader.TestConnectionAsync();
            Assert.False(result);
        }
        if (File.Exists(ValidCsvPath))
            File.Delete(ValidCsvPath);
    }

    /*[Fact]
    public void GetConnection_WithValidCsvFile_ShouldReturnStreamReader()
    {
        _sut.Parameters["FilePath"] = ValidCsvPath;
        File.WriteAllText(ValidCsvPath, "col1,col2\nval1,val2");

        StreamReader? reader = null;
        try
        {
            var obj = _sut.GetConnection();
            reader = obj as StreamReader;
            Assert.NotNull(reader);
            Assert.IsType<StreamReader>(reader);
        }
        finally
        {
            reader?.Dispose();
            if (File.Exists(ValidCsvPath))
                File.Delete(ValidCsvPath);
        }
    }

    [Fact]
    public void GetConnection_WithMissingFilePath_ShouldThrowInvalidOperationException()
    {
        _sut.Parameters.Clear();
        var ex = Assert.Throws<InvalidOperationException>(() => _sut.GetConnection());
        Assert.Equal("File path is required", ex.Message);
    }

    [Fact]
    public void GetConnection_WithNonExistentFile_ShouldThrowFileNotFoundException()
    {
        _sut.Parameters["FilePath"] = NonExistentPath;
        var ex = Assert.Throws<FileNotFoundException>(() => _sut.GetConnection());
        Assert.Equal("CSV file not found", ex.Message);
    }
*/
    [Fact]
    public async Task GetAvailableTables_WithValidCsvFile_ShouldReturnTableInfo()
    {
        //_sut.Parameters["FilePath"] = ValidCsvPath;
        File.WriteAllText(ValidCsvPath, "col1,col2\nval1,val2");

        try
        {
            var reader = connectionBuilder.WithArgs(DataSourceType.CSV, new Dictionary<string, string>
            {
                { "FilePath", ValidCsvPath }
            }).Build();
            var tables = await reader.GetAvailableTables();

            //var tables = await _sut.GetAvailableTables();

            Assert.Single(tables);
            Assert.Equal("test", tables[0].Name);
            Assert.Equal("TABLE", tables[0].Type);
        }
        finally
        {
            if (File.Exists(ValidCsvPath))
                File.Delete(ValidCsvPath);
        }
    }

    [Fact]
    public async Task GetTableSchema_WithValidCsvFile_ShouldReturnColumns()
    {
        // Mock dataReaderFactory to return headers
        var tempFilePath = Path.GetTempFileName();
        

        try
        {
            File.WriteAllText(tempFilePath, "col1,col2\nval1,val2");
            var reader = connectionBuilder.WithArgs(DataSourceType.CSV, new Dictionary<string, string>
            {
                { "FilePath", tempFilePath },
                //{ "HasHeaders", "true" }
            }).Build();
            var schema = await reader.GetTableSchema("test");

            Assert.NotNull(schema);
            Assert.Equal(2, schema.Columns.Count);
            Assert.Equal("col1", schema.Columns[0].Name);
            Assert.Equal("col2", schema.Columns[1].Name);
        }
        finally
        {
            if (File.Exists(tempFilePath))
                File.Delete(tempFilePath);
        }
    }

    [Fact]
    public async Task GetTableSchema_WithEmptyHeaders_ShouldReturnNoColumns()
    {

        var tempFilePath = Path.GetTempFileName();
        try
        {

            File.WriteAllText(tempFilePath, "");
            var reader = connectionBuilder.WithArgs(DataSourceType.CSV, new Dictionary<string, string>
            {
                { "FilePath", tempFilePath }
            }).Build();
            var schema = await reader.GetTableSchema("test");

            Assert.NotNull(schema);
            Assert.Empty(schema.Columns);
        }
        finally
        {
            if (File.Exists(tempFilePath))
                File.Delete(tempFilePath);
        }
    }

    public void Dispose()
    {
        // Ensure all file handles are released before deleting
        GC.Collect();
        GC.WaitForPendingFinalizers();

        if (File.Exists(ValidCsvPath))
            File.Delete(ValidCsvPath);
        if (File.Exists(InvalidExtensionPath))
            File.Delete(InvalidExtensionPath);
        if (File.Exists(NonExistentPath))
            File.Delete(NonExistentPath);
    }

    // Mock for IDataReader
    /*private class MockCsvDataReader : Application.Interfaces.Import.IDataReader
    {
        private readonly IEnumerable<string> _headers;
        public MockCsvDataReader(IEnumerable<string> headers) => _headers = headers;
        public IEnumerable<string> GetHeaders() => _headers;
        public void Dispose() { }
        public string Name => "";
        public long RowCount => 0;
        public long DuplicateHeaderCount => 0;

        public List<string> ErrorMessage => [];

        public Task<IAsyncEnumerable<IDictionary<string, object>>> ReadRowsAsync(int maxDegreeOfParallelism = 4, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        public Task<IEnumerable<IDictionary<string, object>>> ReadPreviewBatchAsync(DataImportOptions options, IColumnFilter columnFilter, CancellationToken cancellationToken) => throw new NotImplementedException();
    }*/
}
