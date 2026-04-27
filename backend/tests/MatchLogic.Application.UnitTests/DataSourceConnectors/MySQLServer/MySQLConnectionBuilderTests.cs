using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MySqlConnector;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.MySQLServer;
public class MySQLConnectionBuilderTests : IDisposable
{
    #region Keys
    private const string ServerKey = "Server";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string AuthTypeKey = "AuthType";
    private const string PortKey = "Port";
    private const string TrustServerCertificateKey = "TrustServerCertificate";
    private const string ConnectionTimeoutKey = "ConnectionTimeout";
    #endregion

    private readonly MySQLConnectionConfig _sut;
    private readonly string TestDatabase = "MatchLogictest1";
    private const string TestHost = "localhost";
    private const string TestUsername = "root";
    private const string TestPassword = "admin123";
    private const string TestPort = "3306";
    private readonly IConnectionBuilder connectionBuilder;
    private readonly IColumnFilter columnFilter;

    public MySQLConnectionBuilderTests()
    {
        _sut = new MySQLConnectionConfig();
        string _dbPath = Path.GetTempFileName();
        string _dbJobPath = Path.GetTempFileName();
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        IServiceCollection services = new ServiceCollection();
        services.AddLogging();
        services.AddApplicationSetup(_dbPath, _dbJobPath);
        var _serviceProvider = services.BuildServiceProvider();
        connectionBuilder = _serviceProvider.GetRequiredService<IConnectionBuilder>();
        columnFilter = _serviceProvider.GetRequiredService<IColumnFilter>();
        TestDatabase = $"MatchLogictest_{Guid.NewGuid():N}";
        SetupTestDatabase();
    }
    private void SetupValidConnectionParameters()
    {
        _sut.Parameters[ServerKey] = TestHost;
        _sut.Parameters[DatabaseKey] = TestDatabase;
        _sut.Parameters[AuthTypeKey] = "SQL";
        _sut.Parameters[UsernameKey] = TestUsername;
        _sut.Parameters[PasswordKey] = TestPassword;
        _sut.Parameters[PortKey] = TestPort;
    }
    private void SetupTestDatabase()
    {
        using var masterConnection = new MySqlConnection(
            $"Server=localhost;port=3306;User Id=root;password=admin123");
        masterConnection.Open();

        // Create test database           
        using var createCommand = new MySqlCommand($@"CREATE DATABASE IF NOT EXISTS {TestDatabase.ToLower()}", masterConnection);
        createCommand.ExecuteNonQuery();

        // Create test tables
        using var testConnection = new MySqlConnection(
            $"Server=localhost;port=3306;database={TestDatabase.ToLower()};User Id=root;password=admin123");
        testConnection.Open();

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
        command.ExecuteNonQuery();
    }
    [Fact]
    public async Task TestConnection_WithValidCredentials_ShouldReturnTrue()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();

        // Act
        var result = await conn.TestConnectionAsync();

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TestConnection_WithInvalidServer_ShouldReturnFalse()
    {
        // Arrange
        SetupValidConnectionParameters();
        _sut.Parameters["Server"] = "invalidserver";
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();
        // Act
        var result = await conn.TestConnectionAsync();

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task GetAvailableTables_ShouldReturnAllTables()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();
        // Act
        var tables = await conn.GetAvailableTables();

        // Assert
        Assert.NotNull(tables);
        Assert.Contains(tables, t => t.Name == "users" && t.Schema == TestDatabase);
        Assert.Contains(tables, t => t.Name == "orders" && t.Schema == TestDatabase);
    }

    [Fact]
    public async Task GetTableSchema_ForUsersTable_ShouldReturnCorrectSchema()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();
        // Act
        //var schema = await conn.GetTableSchema(TestDatabase+".Users");
        var schema = await conn.GetTableSchema("Users");

        // Assert
        Assert.NotNull(schema);
        Assert.NotNull(schema.Columns);
        Assert.Equal(4, schema.Columns.Count);

        var idColumn = schema.Columns.First(c => c.Name == "Id");
        Assert.Equal("System.Int32", idColumn.DataType);
        Assert.False(idColumn.IsNullable);

        var nameColumn = schema.Columns.First(c => c.Name == "Name");
        Assert.Equal("System.String", nameColumn.DataType);  // Updated to match actual type with length
        Assert.True(nameColumn.IsNullable);

        var emailColumn = schema.Columns.First(c => c.Name == "Email");
        Assert.Equal("System.String", emailColumn.DataType); // Updated to match actual type with length
        Assert.False(emailColumn.IsNullable);

        var createdDateColumn = schema.Columns.First(c => c.Name == "CreatedDate");
        Assert.Equal("System.DateTime", createdDateColumn.DataType);
        Assert.True(createdDateColumn.IsNullable);
    }

    [Fact]
    public async Task GetTableSchema_ForOrdersTable_WithSchemaPrefix_ShouldReturnCorrectSchema()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();
        // Act
        //var schema = await conn.GetTableSchema(TestDatabase + ".Orders");
        var schema = await conn.GetTableSchema("Orders");

        // Assert
        Assert.NotNull(schema);
        Assert.NotNull(schema.Columns);
        Assert.Equal(4, schema.Columns.Count);

        var orderIdColumn = schema.Columns.First(c => c.Name == "OrderId");
        Assert.Equal("System.Int32", orderIdColumn.DataType);
        Assert.False(orderIdColumn.IsNullable);

        var amountColumn = schema.Columns.First(c => c.Name == "Amount");
        Assert.Equal("System.Decimal", amountColumn.DataType);
        Assert.True(amountColumn.IsNullable);
    }
    [Fact]
    public async Task GetTableSchema_WithNonexistentTable_ShouldReturnEmptySchema()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();
        // Act
        var schema = await conn.GetTableSchema(TestDatabase + ".NonexistentTable");

        // Assert
        Assert.NotNull(schema);
        Assert.Empty(schema.Columns);
    }

    [Fact]
    public async Task GetAvailableTables_WithSQLAuth_ShouldReturnDatabases()
    {
        // Arrange
        SetupValidConnectionParameters();
        _sut.Parameters[PortKey] = TestPort;
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();
        // Act

        var schema = await (conn as IDataBaseConnectionReaderStrategy).GetAvailableDatabasesAsync();

        // Assert
        Assert.NotNull(schema);
        Assert.True(schema.Count > 0);
        Assert.Contains(TestDatabase, schema);
    }



    [Theory]
    [InlineData("Name, Email","Users", 2)]
    [InlineData("OrderId, UserId, Amount","Orders", 3)]
    public void GetHeaders_WithCustomQuery_ShouldReturnHeaders(string columns,string table, int count)
    {
        // Arrange
        SetupValidConnectionParameters();

        DataSourceConfiguration configuration = new()
        {
            Query = $"SELECT {columns} FROM {TestDatabase}.{table}",
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters, configuration).Build();
        // Act
        var headers = conn.GetHeaders();

        // Assert
        Assert.NotNull(headers);
        Assert.Equal(count, headers.Count());
    }

    [Fact]
    public async Task ReadPreviewBatchAsync_WithoutQueryorTable_ShouldThrowException()
    {
        // Arrange
        SetupValidConnectionParameters();

        var reader = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters).Build();
        //Act
        var exception = await Assert.ThrowsAsync<ArgumentException>(
            async () => await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None));
        //Assert
        Assert.Equal("Either TableName or Query parameter must be provided.", exception.Message);
    }
    [Theory]
    [InlineData("Users",  3)]
    [InlineData("Orders",  4)]
    public async Task ReadPreviewBatchAsync_fromTable_ShouldReturnData(string table, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();

        DataSourceConfiguration sourceConfiguration = new()
        {
            Name = "DS1",
            //TableOrSheet = $"{TestDatabase}.{table}"
            TableOrSheet = table
        };
        var reader = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters, sourceConfiguration).Build();
        //Act
        var result = await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None);
        // Assert
        Assert.Equal(expected, result.Count());
    }

    [Theory]
    [InlineData("Users", 3)]
    [InlineData("Orders", 4)]
    public async Task ReadPreviewBatchAsync_fromQuery_ShouldReturnData(string table, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();
        DataSourceConfiguration configuration = new()
        {
            Query = $"SELECT * FROM {TestDatabase}.{table}",
            Name = "DataSource1"
        };
        var reader = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters, configuration).Build();
        //Act
        var result = await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None);
        // Assert
        Assert.Equal(expected, result.Count());
    }
    [Theory]
    [InlineData("Users", 3)]
    [InlineData("Orders", 4)]
    public void GetRowCount_WithCustomQuery_ShouldReturnCount(string table, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();
        DataSourceConfiguration configuration = new()
        {
            Query = $"SELECT * FROM {TestDatabase}.{table}",
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters, configuration).Build();
        // Act
        var rows = conn.RowCount;

        // Assert
        Assert.Equal(expected, rows);
    }

    [Fact]
    public async Task GetRowCount_WithCustomJoinQuery_ShouldReturnCount()
    {
        // Arrange
        SetupValidConnectionParameters();

        var query = $@"SELECT Id,SUM(Amount) Amount FROM {TestDatabase}.Users
                                        JOIN {TestDatabase}.Orders ON {TestDatabase}.Orders.UserId = {TestDatabase}.Users.Id
                                        GROUP BY Id";
        DataSourceConfiguration configuration = new()
        {
            Query = query,
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.MySQL, _sut.Parameters, configuration).Build();
        // Act
        var rowsEnumerable = await conn.ReadRowsAsync(1, CancellationToken.None);


        // Assert
        Dictionary<string, double> keyValuePairs = new Dictionary<string, double>()
            {
                {"1",300 },
                {"2",600 },
                {"3",160 },
            };
        await foreach (Dictionary<string, object> row in rowsEnumerable.WithCancellation(CancellationToken.None))
        {
            var key = Convert.ToString(row["Id"]);
            var value = Convert.ToDouble(row["Amount"]);

            Assert.Equal(keyValuePairs[key], value);

        }

    }
    //[Theory]
    //[InlineData(null)]
    //[InlineData("")]
    //[InlineData(" ")]
    //public async Task GetTableSchema_WithInvalidTableName_ShouldThrowArgumentException(string tableName)
    //{
    //    // Arrange
    //    SetupValidConnectionParameters();

    //    // Act & Assert
    //    var exception = await Assert.ThrowsAsync<ArgumentException>(
    //        async () => await _sut.GetTableSchema(tableName));
    //    Assert.Equal("Table name cannot be null or empty", exception.Message);
    //}


    public void Dispose()
    {
        // No cleanup needed for these unit tests
        using var connection = new MySqlConnection("Server=localhost;port=3306;User Id=root;password=admin123");
        connection.Open();

        var killSessionsCmd = $@"DROP DATABASE IF EXISTS {TestDatabase.ToLower()};";

        using var command = new MySqlCommand(killSessionsCmd, connection);
        command.ExecuteNonQuery();
    }
}
