using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Project.DataSource;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using Npgsql;
using NPOI.XSSF.UserModel;

namespace MatchLogic.Application.UnitTests;

public class DataSourceServiceTests : IDisposable
{
    private readonly ILogger<DataSource> _logger;
    private readonly DataSourceService _dataSourceService;
    private readonly string _testExcelPath;
    private readonly string _testCSVPath;
    private readonly string _testCurruptedCSVFilePath;
    private readonly string _dsikCSVPath;
    private readonly string _testDuplicateColumnsCSVPath;
    private readonly string _testDuplicateColumnsExcelPath;
    private const string TestServer = "(localdb)\\MSSQLLocalDB";
    private const string TestDatabase = "MatchLogicTest1";
    private readonly IColumnFilter _columnFilter;

    private readonly string _dbPath;
    private IConnectionBuilder connectionBuilder;
    public DataSourceServiceTests()
    {
        //_logger = new NullLogger<DataSource>();

        _columnFilter = new ColumnFilter();

        _testCSVPath = Path.Combine(Path.GetTempPath(), "testCSV1.csv");
        _testDuplicateColumnsCSVPath = Path.Combine(Path.GetTempPath(), "testCSV2.csv");
        _testCurruptedCSVFilePath = Path.Combine(Path.GetTempPath(), "testCurruptedCSV2.csv");


        _testExcelPath = Path.Combine(Path.GetTempPath(), "test1.xlsx");
        _testDuplicateColumnsExcelPath = Path.Combine(Path.GetTempPath(), "test2.xlsx");


        _dbPath = Path.GetTempFileName();
        var _dbJobPath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _logger = loggerFactory.CreateLogger<DataSource>();
        // Create service collection
        IServiceCollection services = new ServiceCollection();
        services.AddLogging();
        services.AddApplicationSetup(_dbPath, _dbJobPath);
        var _serviceProvider = services.BuildServiceProvider();
        connectionBuilder = _serviceProvider.GetRequiredService<IConnectionBuilder>();
        _dataSourceService = new DataSourceService(_logger, _columnFilter, connectionBuilder);

        SetupTestEnvironment().Wait();
    }

    private async Task SetupTestEnvironment()
    {
        CreateTestExcelFile();
        CreateTestCSVFile();
        await SetupSQLServerTestDatabase();
        await SetupPosgresSQLTestDatabase();
        await SetupMySQLTestDatabase();
    }


    #region Excel Tests

    [Fact]
    public async Task ExcelPreview_WithDuplicateColumnNames_ReturnsCorrectData()
    {
        // Arrange
        CreateTestDuplicateColumnsExcelFile();
        var dataSource = CreateDuplicateNameExcelDataSource();
        //var dataSource = CreateExcelDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        Assert.Equal(1, result.DuplicateHeaderCount);
        var firstRow = result.Data.First();
        Assert.Equal("John Doe", firstRow["Name"]);
        Assert.Equal("Ahsan", firstRow["Name_1"]);
        Assert.Equal(30.0, firstRow["Age"]);
    }

    [Fact]
    public async Task ExcelPreview_WithValidFile_ReturnsCorrectData()
    {
        // Arrange
        var dataSource = CreateExcelDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        var firstRow = result.Data.First();
        Assert.Equal("John Doe", firstRow["Name"]);
        Assert.Equal(30.0, firstRow["Age"]);
    }

    [Fact]
    public async Task ExcelConnection_WithValidFile_TestsSuccessfully()
    {
        // Arrange
        var dataSource = CreateExcelDataSource();

        // Act
        var result = await _dataSourceService.TestConnectionAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.True(result.Success);
    }

    [Fact]
    public async Task ExcelMetadata_WithValidFile_ReturnsCorrectSchema()
    {
        // Arrange
        var dataSource = CreateExcelDataSource();

        // Act
        var result = await _dataSourceService.GetMetadataAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Tables[0].Columns);
        Assert.Equal("Sheet1", result.Tables[0].Name);
        Assert.Contains(result.Tables[0].Columns, c => c.Name == "Name");
        Assert.Contains(result.Tables[0].Columns, c => c.Name == "Age");

        Assert.NotNull(result.Tables[1].Columns);
        Assert.Equal("Actors", result.Tables[1].Name);
        Assert.Contains(result.Tables[1].Columns, c => c.Name == "Name");
        Assert.Contains(result.Tables[1].Columns, c => c.Name == "Age");
        Assert.Contains(result.Tables[1].Columns, c => c.Name == "Gender");
    }

    #endregion

    #region CSV Tests

    [Fact]
    public async Task CSVPreview_WithValidFile_ReturnsCorrectData()
    {
        // Arrange
        var dataSource = CreateCSVDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        var firstRow = result.Data.First();
        Assert.Equal("John Doe", firstRow["Name"]);
        Assert.Equal("30", firstRow["Age"]);
    }
    [Fact]
    public async Task CSVPreview_WithDuplicateColumnNames_ReturnsCorrectData()
    {
        // Arrange
        CreateTestDuplicateColumnsCSVFile();
        var dataSource = CreateDuplicateNameCSVDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        Assert.Equal(1, result.DuplicateHeaderCount);
        var firstRow = result.Data.First();
        Assert.Equal("John Doe", firstRow["Name"]);
        Assert.Equal("Ahsan", firstRow["Name_1"]);
        Assert.Equal("30", firstRow["Age"]);
    }


    [Fact]
    public async Task CSVPreview_WithCurrptedColumns_ReturnsCorrectData()
    {
        // Arrange
        //CreateTestDuplicateColumnsCSVFile();
        var dataSource = CreateCurruptedCSVDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        Assert.Equal(0, result.DuplicateHeaderCount);
        var firstRow = result.Data.First();
        Assert.Equal("", firstRow["Name"]);
        Assert.Equal("25", firstRow["Age"]);
    }

    [Fact]
    public async Task CSVConnection_WithValidFile_TestsSuccessfully()
    {
        // Arrange
        var dataSource = CreateExcelDataSource();

        // Act
        var result = await _dataSourceService.TestConnectionAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.True(result.Success);
    }

    [Fact]
    public async Task CSVMetadata_WithValidFile_ReturnsCorrectSchema()
    {
        // Arrange
        var dataSource = CreateCSVDataSource();

        // Act
        var result = await _dataSourceService.GetMetadataAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Tables[0].Columns);
        Assert.Contains(result.Tables[0].Columns, c => c.Name == "Name");
        Assert.Contains(result.Tables[0].Columns, c => c.Name == "Age");
    }


    [Fact]
    public async Task CSV_WithBadFile_ReturnsCorrectSchema()
    {
        // Arrange
        var dataSource = CreateBadCSVDataSource();

        // Act
        //var result = await _sut.GetMetadataAsync(dataSource, CancellationToken.None);
        //var result = await _sut.GetMetadataAsync(dataSource, CancellationToken.None);
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);


        // Assert
        Assert.NotNull(result);
        Assert.True(result.ErrorMessages?.Count > 0);
        //Assert.Contains(result.Tables[0].Columns, c => c.Name == "Name");
        //Assert.Contains(result.Tables[0].Columns, c => c.Name == "Age");
    }
    #endregion

    #region SQL Server Tests

    [Fact]
    public async Task SqlServer_WithValidConnection_TestsSuccessfully()
    {
        // Arrange
        var dataSource = CreateSqlServerDataSource();

        // Act
        var result = await _dataSourceService.TestConnectionAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.True(result.Success);
    }

    [Fact]
    public async Task SqlServerMetadata_ReturnsCorrectSchema()
    {
        // Arrange
        var dataSource = CreateSqlServerDataSource();

        // Act
        var result = await _dataSourceService.GetMetadataAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Tables);
        Assert.Contains(result.Tables, t => t.Name == "Users");

        var columns = result.Tables[0].Columns;
        Assert.NotNull(columns);
        Assert.Contains(columns, c => c.Name == "Id");
        Assert.Contains(columns, c => c.Name == "Name");
        Assert.Contains(columns, c => c.Name == "Email");


        var column2 = result.Tables[1].Columns;
        Assert.NotNull(column2);
        Assert.Contains(column2, c => c.Name == "UserId");
        Assert.Contains(column2, c => c.Name == "OrderDate");
        Assert.Contains(column2, c => c.Name == "Amount");
    }

    [Fact]
    public async Task SqlServerPreview_ReturnsCorrectData()
    {
        // Arrange
        var dataSource = CreateSqlServerDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        Assert.True(result.Data.Count <= 100); // Max preview records
    }

    #endregion

    #region PostgreSQL Tests


    [Fact]
    public async Task PostgreSQL_WithValidConnection_TestsSuccessfully()
    {
        // Arrange
        var dataSource = CreatePostGreSqlDataSource();

        // Act
        var result = await _dataSourceService.TestConnectionAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.True(result.Success);
    }
    [Fact]
    public async Task PostgreSQLMetadata_ReturnsCorrectSchema()
    {
        // Arrange
        var dataSource = CreatePostGreSqlDataSource();

        // Act
        var result = await _dataSourceService.GetMetadataAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Tables);
        Assert.Contains(result.Tables, t => t.Name == "Users");

        var columns = result.Tables[0].Columns;
        Assert.NotNull(columns);
        Assert.Contains(columns, c => c.Name == "Id");
        Assert.Contains(columns, c => c.Name == "Name");
        Assert.Contains(columns, c => c.Name == "Email");


        var column2 = result.Tables[1].Columns;
        Assert.NotNull(column2);
        Assert.Contains(column2, c => c.Name == "UserId");
        Assert.Contains(column2, c => c.Name == "OrderDate");
        Assert.Contains(column2, c => c.Name == "Amount");
    }

    [Fact]
    public async Task PostgreSQLPreview_ReturnsCorrectData()
    {
        // Arrange
        var dataSource = CreatePostGreSqlDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        Assert.True(result.Data.Count <= 100); // Max preview records
    }
    #endregion

    #region MySQL Tests
    [Fact]
    public async Task MySQL_WithValidConnection_TestsSuccessfully()
    {
        // Arrange
        var dataSource = CreateMySqlDataSource();

        // Act
        var result = await _dataSourceService.TestConnectionAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.True(result.Success);
    }

    [Fact]
    public async Task MySQLMetadata_ReturnsCorrectSchema()
    {
        // Arrange
        var dataSource = CreateMySqlDataSource();

        // Act
        var result = await _dataSourceService.GetMetadataAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Tables);
        Assert.Contains(result.Tables, t => t.Name == "users");

        var columns = result.Tables[1].Columns;
        Assert.NotNull(columns);
        Assert.Contains(columns, c => c.Name == "Id");
        Assert.Contains(columns, c => c.Name == "Name");
        Assert.Contains(columns, c => c.Name == "Email");


        var column2 = result.Tables[0].Columns;
        Assert.NotNull(column2);
        Assert.Contains(column2, c => c.Name == "UserId");
        Assert.Contains(column2, c => c.Name == "OrderDate");
        Assert.Contains(column2, c => c.Name == "Amount");
    }

    [Fact]
    public async Task MySQLPreview_ReturnsCorrectData()
    {
        // Arrange
        var dataSource = CreateMySqlDataSource();

        // Act
        var result = await _dataSourceService.PreviewDataSourceAsync(dataSource, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.NotNull(result.Data);
        Assert.NotEmpty(result.Data);
        Assert.True(result.Data.Count <= 100); // Max preview records
    }
    #endregion
    #region Helper Methods

    private DataSource CreateCSVDataSource()
    {
        //File.WriteAllText(_testCSVPath, "Name,Age\nJohn Doe\nJane Smith");
        //File.WriteAllText(_testCSVPath, "Name,Age\nJohn Doe,30\nJane Smith,25");
        //File.WriteAllText(_testCSVPath, "Name,Age,Name\nJohn Doe,30,Haisam\nJane Smith,25,Ahsan");
        var connectionInfo = new BaseConnectionInfo()
        {
            //dataReaderFactory = excelFunc,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", _testCSVPath },
                { "HasHeaders", "true" },
                { "Delimiter", "," },
                { "Encoding", "UTF8" },
                //{ "Quote", "\"" },
                //{ "Comment", "@" },
            }
        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.CSV,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "",
                ColumnMappings = new Dictionary<string, ColumnMapping>
                {
                    { "Name", new ColumnMapping { SourceColumn = "Name", TargetColumn = "Name", Include = true } },
                    { "Age", new ColumnMapping { SourceColumn = "Age", TargetColumn = "Age", Include = true } }
                }
            }
        };
    }

    private DataSource CreateBadCSVDataSource()
    {
        //File.WriteAllText(_testCSVPath, "Name,Age\nJohn Doe\nJane Smith");
        //File.WriteAllText(_testCSVPath, "Name,Age\nJohn Doe,30\nJane Smith,25");
        File.WriteAllText(_testCSVPath, "\"Words\",\"email@email.com\",\"\",\"4253\",\"57574\",\"FirstName\",\"\",\"LastName, MD\",\"\",\"\",\"576JFJD\",\"\",\"1971\",\"\",\"Words\",\"Address\",\"SUITE \"A\"\",\"City\",\"State\",\"Zip\",\"Phone\",\"\",\"\"");
        var connectionInfo = new BaseConnectionInfo()
        {
            //dataReaderFactory = excelFunc,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", _testCSVPath },
                { "HasHeaders", "true" },
                { "Delimiter", "," },
                { "Encoding", "UTF8" },
                //{ "Quote", "\"" },
                //{ "Comment", "@" },
            }
        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.CSV,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "",
                ColumnMappings = new Dictionary<string, ColumnMapping>
                {
                }
            }
        };
    }
    private DataSource CreateDuplicateNameCSVDataSource()
    {
        var connectionInfo = new BaseConnectionInfo()
        {
            //dataReaderFactory = excelFunc,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", _testDuplicateColumnsCSVPath },
                { "HasHeaders", "true" },
                { "Delimiter", "," },
            }
        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.CSV,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "",
            }
        };
    }

    private DataSource CreateCurruptedCSVDataSource()
    {
        var connectionInfo = new BaseConnectionInfo()
        {
            //dataReaderFactory = excelFunc,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", _testCurruptedCSVFilePath },
                { "HasHeaders", "true" },
                { "Delimiter", "," },
            }
        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.CSV,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "",
            }
        };
    }
    private DataSource CreateExcelDataSource()
    {
        var connectionInfo = new BaseConnectionInfo()
        {
            //dataReaderFactory = excelFunc,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", _testExcelPath },
                { "HasHeaders", "true" }
            }
        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.Excel,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "",
                ColumnMappings = new Dictionary<string, ColumnMapping>
                {
                    { "Name", new ColumnMapping { SourceColumn = "Name", TargetColumn = "Name", Include = true } },
                    { "Age", new ColumnMapping { SourceColumn = "Age", TargetColumn = "Age", Include = true } }
                }
            }
        };
    }

    private DataSource CreateDuplicateNameExcelDataSource()
    {
        var connectionInfo = new BaseConnectionInfo()
        {
            //dataReaderFactory = excelFunc,
            Parameters = new Dictionary<string, string>
            {
                { "FilePath", _testDuplicateColumnsExcelPath },
                { "HasHeaders", "true" }
            }
        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.Excel,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "",
            }
        };
    }

    private DataSource CreatePostGreSqlDataSource()
    {
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
            {
                { "Server", "localhost" },
                { "Database", TestDatabase.ToLower() },
                { "AuthType", "SQL" },
                { "Username", "postgres" },
                { "Password", "admin123" }
            },

        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.PostgreSQL,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "public.Users",
                ColumnMappings = new Dictionary<string, ColumnMapping>
                {
                    { "Id", new ColumnMapping { SourceColumn = "Id", TargetColumn = "Id", Include = true } },
                    { "Name", new ColumnMapping { SourceColumn = "Name", TargetColumn = "Name", Include = true } },
                    { "Email", new ColumnMapping { SourceColumn = "Email", TargetColumn = "Email", Include = true } }
                }
            }
        };
    }
    private DataSource CreateMySqlDataSource()
    {
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
            {
                { "Server", "localhost" },
                { "Database", $"{TestDatabase.ToLower()}" },
                { "AuthType", "SQL" },
                { "Username", "root" },
                { "Password", "admin123" },
                { "Port", "3306" }
            },

        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.MySQL,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = $"{TestDatabase.ToLower()}.Users",
                ColumnMappings = new Dictionary<string, ColumnMapping>
                {
                    { "Id", new ColumnMapping { SourceColumn = "Id", TargetColumn = "Id", Include = true } },
                    { "Name", new ColumnMapping { SourceColumn = "Name", TargetColumn = "Name", Include = true } },
                    { "Email", new ColumnMapping { SourceColumn = "Email", TargetColumn = "Email", Include = true } }
                }
            }
        };
    }
    private DataSource CreateSqlServerDataSource()
    {
        var connectionInfo = new BaseConnectionInfo
        {
            Parameters = new Dictionary<string, string>
            {
                { "Server", TestServer },
                { "Database", TestDatabase },
                { "AuthType", "Windows" }
            },

        };
        return new DataSource
        {
            Id = Guid.NewGuid(),
            Type = DataSourceType.SQLServer,
            ConnectionDetails = connectionInfo,
            Configuration = new DataSourceConfiguration
            {
                TableOrSheet = "dbo.Users",
                ColumnMappings = new Dictionary<string, ColumnMapping>
                {
                    { "Id", new ColumnMapping { SourceColumn = "Id", TargetColumn = "Id", Include = true } },
                    { "Name", new ColumnMapping { SourceColumn = "Name", TargetColumn = "Name", Include = true } },
                    { "Email", new ColumnMapping { SourceColumn = "Email", TargetColumn = "Email", Include = true } }
                }
            }
        };
    }
    private void CreateTestDuplicateColumnsExcelFile()
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");

        var sheet2 = workbook.CreateSheet("Sheet2");

        // Create header row
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("Name");
        headerRow.CreateCell(1).SetCellValue("Age");
        headerRow.CreateCell(2).SetCellValue("Name");

        // Add test data
        var row1 = sheet.CreateRow(1);
        row1.CreateCell(0).SetCellValue("John Doe");
        row1.CreateCell(1).SetCellValue(30);
        row1.CreateCell(2).SetCellValue("Ahsan");

        var row2 = sheet.CreateRow(2);
        row2.CreateCell(0).SetCellValue("Jane Smith");
        row2.CreateCell(1).SetCellValue(25);
        row2.CreateCell(2).SetCellValue("Haisam");

        using var fileStream = File.Create(_testDuplicateColumnsExcelPath);
        workbook.Write(fileStream);
    }

    private void CreateTestCSVFile()
    {

        File.WriteAllText(_testCSVPath, "Name,Age\nJohn Doe,30\nJane Smith,25\nBrad Pitt,42");
        CreateTestDuplicateColumnsCSVFile();
        CreateTestCurruptedCSVFile();
        //File.WriteAllText(_testCSVPath, "Header1,Header2,Header3,Header4,Header5,Header6,Header7,Header8,Header9,Header10,Header11,Header12,Header13,Header14,Header15,Header16,Header17,Header18\r\n'Item1','item2','Item3',\"Item4\",Item5,Item6,Item7,'Item8',Item9,Item10,Item11,Item12,Item13,Item14,Item15,'Item16',Item17,Item18\r\n'Item21','item22','Item23',\"Item24\",Item25,Item26,'Item27',Item28,Item29,Item30,Item31,Item32,Item33,Item34,'Item35',Item36,Item37,Item38\r\n'Item31','item32','Item33',\"Item34\",Item35,Item36,'Item37',Item38,Item39,Item40,Item41,Item42,Item43,Item44,'Item45',Item46,Item47,Item48");

    }

    private void CreateTestDuplicateColumnsCSVFile()
    {
        File.WriteAllText(_testDuplicateColumnsCSVPath, "Name,Age,Name\nJohn Doe,30,Ahsan\nJane Smith,25,Haisam");

    }

    private void CreateTestCurruptedCSVFile()
    {
        File.WriteAllText(_testCurruptedCSVFilePath, "Name,Age\nJohn Doe\n,25\nBrad Pitt,42");

    }
    private void CreateTestExcelFile()
    {
        using var workbook = new XSSFWorkbook();
        var sheet = workbook.CreateSheet("Sheet1");



        // Create header row
        var headerRow = sheet.CreateRow(0);
        headerRow.CreateCell(0).SetCellValue("Name");
        headerRow.CreateCell(1).SetCellValue("Age");

        // Add test data
        var row1 = sheet.CreateRow(1);
        row1.CreateCell(0).SetCellValue("John Doe");
        row1.CreateCell(1).SetCellValue(30);

        var row2 = sheet.CreateRow(2);
        row2.CreateCell(0).SetCellValue("Jane Smith");
        row2.CreateCell(1).SetCellValue(25);




        // Add test data for Sheet 2

        var sheet2 = workbook.CreateSheet("Actors");

        var Sheet2headerRow = sheet2.CreateRow(0);
        Sheet2headerRow.CreateCell(0).SetCellValue("Name");
        Sheet2headerRow.CreateCell(1).SetCellValue("Age");
        Sheet2headerRow.CreateCell(2).SetCellValue("Gender");

        var sheet2Row1 = sheet2.CreateRow(1);
        sheet2Row1.CreateCell(0).SetCellValue("Jorge Coloney");
        sheet2Row1.CreateCell(1).SetCellValue(30);
        sheet2Row1.CreateCell(2).SetCellValue("Male");

        var sheet2Row2 = sheet2.CreateRow(2);
        sheet2Row2.CreateCell(0).SetCellValue("Brad Smith");
        sheet2Row2.CreateCell(1).SetCellValue(25);
        sheet2Row2.CreateCell(2).SetCellValue("Male");

        sheet2Row2 = sheet2.CreateRow(3);
        sheet2Row2.CreateCell(0).SetCellValue("Scralett Jhonson");
        sheet2Row2.CreateCell(1).SetCellValue(25);
        sheet2Row2.CreateCell(2).SetCellValue("Female");

        using var fileStream = File.Create(_testExcelPath);
        workbook.Write(fileStream);
    }

    private async Task SetupSQLServerTestDatabase()
    {
        using var masterConnection = new Microsoft.Data.SqlClient.SqlConnection(
            $"Server={TestServer};Database=master;Integrated Security=True");
        await masterConnection.OpenAsync();

        // Create test database
        var createDbCmd = $@"
                IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{TestDatabase}')
                BEGIN
                    CREATE DATABASE {TestDatabase}
                END";
        using (var commanda = new Microsoft.Data.SqlClient.SqlCommand(createDbCmd, masterConnection))
        {
            await commanda.ExecuteNonQueryAsync();
        }

        // Create test tables
        using var testConnection = new Microsoft.Data.SqlClient.SqlConnection(
            $"Server={TestServer};Database={TestDatabase.ToLower()};Integrated Security=True");
        await testConnection.OpenAsync();

        var createTableCmd = @"
                IF OBJECT_ID('dbo.Users', 'U') IS NULL
                CREATE TABLE dbo.Users (
                    Id INT PRIMARY KEY IDENTITY(1,1),
                    Name NVARCHAR(100) NULL,
                    Email NVARCHAR(255) NOT NULL
                )
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test')
                    EXEC('CREATE SCHEMA test')
                IF OBJECT_ID('test.Orders', 'U') IS NULL
                CREATE TABLE test.Orders (
                    OrderId INT PRIMARY KEY IDENTITY(1,1),
                    UserId INT NOT NULL,
                    OrderDate DATETIME2 NOT NULL,
                    Amount DECIMAL(18,2) NULL
                )
                IF NOT EXISTS (SELECT * FROM dbo.Users)
                BEGIN
                    INSERT INTO dbo.Users (Name, Email) VALUES
                    ('John Doe', 'john@example.com'),
                    ('Jane Smith', 'jane@example.com')
                END";

        using var command = new Microsoft.Data.SqlClient.SqlCommand(createTableCmd, testConnection);
        await command.ExecuteNonQueryAsync();
    }
    private async Task SetupPosgresSQLTestDatabase()
    {
        using var masterConnection = new NpgsqlConnection("host=localhost;port=5432;database=postgres;username=postgres;password=admin123");
        await masterConnection.OpenAsync();

        // Create test database
        int dbExists = 0;
        using (var commanda = new NpgsqlCommand($@"SELECT 1 FROM pg_database WHERE datname = '{TestDatabase.ToLower()}'", masterConnection))
        {
            var result = await commanda.ExecuteScalarAsync();
            dbExists = result != null ? (int)result : 0;
        }

        if (dbExists <= 0)
        {
            using var createCommand = new NpgsqlCommand($@"CREATE DATABASE {TestDatabase.ToLower()}", masterConnection);
            await createCommand.ExecuteNonQueryAsync();
        }

        // Create test tables
        using var testConnection = new NpgsqlConnection(
            $"host=localhost;port=5432;database={TestDatabase.ToLower()};username=postgres;password=admin123");
        await testConnection.OpenAsync();

        var createTableCmd = @"
        -- Create schema if not exists
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'test') THEN
                    EXECUTE 'CREATE SCHEMA test';
                END IF;
            END$$;

            -- Create Users table if not exists
            CREATE TABLE IF NOT EXISTS public.""Users"" (
                ""Id"" SERIAL PRIMARY KEY,
                ""Name"" VARCHAR(100),
                ""Email"" VARCHAR(255) NOT NULL
            );

            -- Create Orders table if not exists
            CREATE TABLE IF NOT EXISTS test.""Orders"" (
                ""OrderId"" SERIAL PRIMARY KEY,
                ""UserId"" INT NOT NULL,
                ""OrderDate"" TIMESTAMP NOT NULL,
                ""Amount"" NUMERIC(18,2)
            );

            -- Insert data only if Users table is empty
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM public.""Users"") THEN
                    INSERT INTO public.""Users"" (""Name"", ""Email"") VALUES
                    ('John Doe', 'john@example.com'),
                    ('Jane Smith', 'jane@example.com');
                END IF;
            END$$;";

        using var command = new NpgsqlCommand(createTableCmd, testConnection);
        await command.ExecuteNonQueryAsync();
    }
    private async Task SetupMySQLTestDatabase()
    {
        using var masterConnection = new MySqlConnection(
            $"Server=localhost;port=3306;User Id=root;password=admin123");
        await masterConnection.OpenAsync();

        // Create test database           
        using var createCommand = new MySqlCommand($@"CREATE DATABASE IF NOT EXISTS {TestDatabase.ToLower()}", masterConnection);
        await createCommand.ExecuteNonQueryAsync();

        // Create test tables
        using var testConnection = new MySqlConnection(
            $"Server=localhost;port=3306;database={TestDatabase.ToLower()};User Id=root;password=admin123");
        await testConnection.OpenAsync();

        var createTableCmd = @$"                   
                    -- Create Users table if not exists
                    CREATE TABLE IF NOT EXISTS Users (
                        Id INT AUTO_INCREMENT PRIMARY KEY,
                        Name VARCHAR(100) NULL,
                        Email VARCHAR(255) NOT NULL,
                        CreatedDate DATETIME DEFAULT CURRENT_TIMESTAMP
                    );


                    -- Create Orders table if not exists
                    CREATE TABLE IF NOT EXISTS Orders (
                        OrderId INT AUTO_INCREMENT PRIMARY KEY,
                        UserId INT NOT NULL,
                        OrderDate DATETIME NOT NULL,
                        Amount DECIMAL(18,2) NULL
                    );

                    -- Insert initial data only if empty
                    INSERT INTO Users (Name, Email)
                    SELECT 'John Doe', 'john@example.com'
                    WHERE NOT EXISTS (SELECT 1 FROM Users LIMIT 1);

                    INSERT INTO Users (Name, Email)
                    VALUES ('Jane Smith', 'jane@example.com');

                    -- Additional test data
                    INSERT INTO Users (Name, Email)
                    VALUES 
                    ('TestUser1', 'demo1@mail.com'),
                    ('TestUser2', 'demo2@mail.com'),
                    ('TestUser3', 'demo3@mail.com');

                    -- Insert Orders
                    INSERT INTO Orders (UserId, OrderDate, Amount)
                    VALUES 
                    (1, NOW(), 250),
                    (2, NOW(), 600),
                    (3, NOW(), 160),
                    (1, NOW(), 50);";

        using var command = new MySqlCommand(createTableCmd, testConnection);
        await command.ExecuteNonQueryAsync();
    }


    #endregion
    public void Dispose()
    {
        // Clean up Excel file
        if (File.Exists(_testExcelPath))
        {
            try
            {
                File.Delete(_testExcelPath);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        if (File.Exists(_testCSVPath))
        {
            try
            {
                File.Delete(_testCSVPath);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        // Clean up database
        CleanSQLServerDataBase();
        CleanPosgreDatabase();
        CleanMysqlDatabase();
    }
    private void CleanSQLServerDataBase()
    {

        using var connection = new Microsoft.Data.SqlClient.SqlConnection(
                $"Server={TestServer};Database=master;Integrated Security=True");
        connection.Open();

        var killSessionsCmd = $@"
                IF EXISTS (SELECT * FROM sys.databases WHERE name = '{TestDatabase}')
                BEGIN
                    ALTER DATABASE {TestDatabase} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE {TestDatabase};
                END";

        using var command = new Microsoft.Data.SqlClient.SqlCommand(killSessionsCmd, connection);
        command.ExecuteNonQuery();
    }
    private void CleanPosgreDatabase()
    {
        using var connection = new NpgsqlConnection(
            $"host=localhost;port=5432;database=postgres;username=postgres;password=admin123");
        connection.Open();

        var killSessionsCmd = $@"
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = '{TestDatabase}') THEN
                        -- Terminate all active connections to the target database
                        PERFORM pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = '{TestDatabase}'
                          AND pid <> pg_backend_pid();

                        -- Now drop the database
                        EXECUTE 'DROP DATABASE {TestDatabase}';
                    END IF;
                END$$;
";

        using var command = new NpgsqlCommand(killSessionsCmd, connection);
        command.ExecuteNonQuery();
    }
    private void CleanMysqlDatabase()
    {

        using var connection = new MySqlConnection("Server=localhost;port=3306;User Id=root;password=admin123");
        connection.Open();

        var killSessionsCmd = $@"DROP DATABASE IF EXISTS {TestDatabase.ToLower()};";

        using var command = new MySqlCommand(killSessionsCmd, connection);
        command.ExecuteNonQuery();
    }
}
