using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.MSSQLServer;
public class SqlServerConnectionBuilderTests : IDisposable
{
    #region  Keys
    private const string ServerKey = "Server";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string AuthTypeKey = "AuthType";

    private const string TableNameKey = "TableName";
    private const string SchemaNameKey = "SchemaName";
    private const string QueryKey = "Query";
    #endregion

    private readonly SQLServerConnectionConfig _sut;
    private const string TestServer = "(localdb)\\MSSQLLocalDB"; // Using LocalDB for tests
    private readonly string TestDatabase = "MatchLogicTest";
    private readonly IConnectionBuilder connectionBuilder;
    private readonly IColumnFilter columnFilter;
    public SqlServerConnectionBuilderTests()
    {
        _sut = new SQLServerConnectionConfig();
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
        SetupTestDatabase().Wait();
    }

    private void SetupValidConnectionParameters()
    {
        _sut.Parameters["Server"] = TestServer;
        _sut.Parameters["Database"] = TestDatabase;
        _sut.Parameters["AuthType"] = "Windows";
    }

    private async Task SetupTestDatabase()
    {
        // Connect to master to create test database
        using var masterConnection = new SqlConnection(
            $"Server={TestServer};Database=master;Integrated Security=True");
        await masterConnection.OpenAsync();

        // Create test database if it doesn't exist
        var createDbCmd = $@"
                IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{TestDatabase}')
                BEGIN
                    CREATE DATABASE {TestDatabase}
                END";
        using (var command = new SqlCommand(createDbCmd, masterConnection))
        {
            await command.ExecuteNonQueryAsync();
        }

        // Create test tables
        using var testConnection = new SqlConnection(
            $"Server={TestServer};Database={TestDatabase};Integrated Security=True");
        await testConnection.OpenAsync();

        var createTableCmd = @"
                IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'test')
                    EXEC('CREATE SCHEMA test')

                IF OBJECT_ID('dbo.Users', 'U') IS NULL
                CREATE TABLE dbo.Users (
                    Id INT PRIMARY KEY IDENTITY(1,1),
                    Name NVARCHAR(100) NULL,
                    Email NVARCHAR(255) NOT NULL,
                    CreatedDate DATETIME2 DEFAULT GETDATE()
                )

                IF OBJECT_ID('test.Orders', 'U') IS NULL
                CREATE TABLE test.Orders (
                    OrderId INT PRIMARY KEY IDENTITY(1,1),
                    UserId INT NOT NULL,
                    OrderDate DATETIME2 NOT NULL,
                    Amount DECIMAL(18,2) NULL
                )";

        using (var command = new SqlCommand(createTableCmd, testConnection))
        {
            await command.ExecuteNonQueryAsync();
        }

        var addSqlUserCmd = @"
                IF NOT EXISTS (SELECT * FROM sys.sql_logins WHERE name = 'sadmin')
                BEGIN
                    CREATE LOGIN sadmin WITH PASSWORD = 'admin123';
                    CREATE USER sadmin FOR LOGIN sadmin;
                    EXEC sp_addrolemember 'db_owner', 'sadmin'
                END";

        using (var command = new SqlCommand(addSqlUserCmd, testConnection))
        {
            await command.ExecuteNonQueryAsync();
        }

        DemoData();
    }

    private void DemoData()
    {
        using var connection = new SqlConnection(
            $"Server={TestServer};Database={TestDatabase};Integrated Security=True");
        connection.Open();
        var usersQuery = @"INSERT INTO [dbo].[Users] ([Name],[Email],[CreatedDate])
                     VALUES ('TestUser1','demo1@mail.com',GETDATE()),
                           ('TestUser2','demo2@mail.com',GETDATE()),
                           ('TestUser3','demo3@mail.com',GETDATE())";
        var ordersQuery = @"INSERT INTO [test].[Orders] ([UserId],[OrderDate],[Amount])
                VALUES (1,GETDATE(),250),(2,GETDATE(),600),(3,GETDATE(),160),(1,GETDATE(),50)";

        using var user_command = new SqlCommand(usersQuery, connection);
        user_command.ExecuteNonQuery();

        using var order_command = new SqlCommand(ordersQuery, connection);
        order_command.ExecuteNonQuery();

    }

    [Fact]
    public async Task TestConnection_WithValidCredentials_ShouldReturnTrue()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();

        // Act
        var result = await conn.TestConnectionAsync();

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TestConnection_WithInvalidServer_ShouldReturnFalse()
    {
        // Arrange
        _sut.Parameters["Server"] = "invalidserver";
        _sut.Parameters["Database"] = TestDatabase;
        _sut.Parameters["AuthType"] = "Windows";
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
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
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
        // Act
        var tables = await conn.GetAvailableTables();

        // Assert
        Assert.NotNull(tables);
        Assert.Contains(tables, t => t.Name == "Users" && t.Schema == "dbo");
        Assert.Contains(tables, t => t.Name == "Orders" && t.Schema == "test");
    }

    [Theory]
    [InlineData("[dbo].[Users]")] 
    [InlineData("[Users]")] 
    [InlineData("dbo.Users")]
    [InlineData("Users")]
    [InlineData(".Users")]
    public async Task GetTableSchema_ForUsersTable_ShouldReturnCorrectSchema(string tableName)
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
        // Act
        var schema = await conn.GetTableSchema(tableName);

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

    [Theory]
    [InlineData("[test].[Orders]")]
    [InlineData("test.Orders")]
    public async Task GetTableSchema_ForOrdersTable_WithSchemaPrefix_ShouldReturnCorrectSchema(string tableName)
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
        // Act
        var schema = await conn.GetTableSchema(tableName);

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

    [Fact]
    public async Task GetTableSchema_WithNonexistentTable_ShouldReturnEmptySchema()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
        // Act
        var schema = await conn.GetTableSchema("dbo.NonexistentTable");

        // Assert
        Assert.NotNull(schema);
        Assert.Empty(schema.Columns);
    }


    [Fact]
    public async Task GetAvailableTables_WithWindowsAuth_ShouldReturnDatabases()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
        // Act

        var schema = await (conn as IDataBaseConnectionReaderStrategy).GetAvailableDatabasesAsync();

        // Assert
        Assert.NotNull(schema);
        Assert.True(schema.Count > 0);
        Assert.Contains(TestDatabase, schema);
    }
    [Fact]
    public async Task GetAvailableTables_WithSQLAuth_ShouldReturnDatabases()
    {
        // Arrange
        SetupValidConnectionParameters();
        _sut.Parameters["AuthType"] = "SQL";
        _sut.Parameters["Username"] = "sadmin";
        _sut.Parameters["Password"] = "admin123";
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
        // Act

        var schema = await (conn as IDataBaseConnectionReaderStrategy).GetAvailableDatabasesAsync();

        // Assert
        Assert.NotNull(schema);
        Assert.True(schema.Count > 0);
        Assert.Contains(TestDatabase, schema);
    }



    [Fact]
    public async Task GetHeaders_WithCustomQuery_ShouldReturnHeaders()
    {
        // Arrange
        SetupValidConnectionParameters();

        _sut.Parameters[QueryKey] = "SELECT [Name],[Email] FROM [dbo].[Users]";
        DataSourceConfiguration configuration = new()
        {
            Query = "SELECT [Name],[Email] FROM [dbo].[Users]",
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters, configuration).Build();
        // Act
        var headers = conn.GetHeaders();

        // Assert
        Assert.NotNull(headers);
        Assert.Equal(2, headers.Count());
    }



    [Fact]
    public async Task ReadPreviewBatchAsync_WithoutQueryorTable_ShouldThrowException()
    {
        // Arrange
        SetupValidConnectionParameters();

        var reader = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters).Build();
        //Act
        var exception = await Assert.ThrowsAsync<ArgumentException>(
            async () => await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None));
        //Assert
        Assert.Equal("Either TableName or Query parameter must be provided.", exception.Message);
    }
    [Theory]
    [InlineData("Users", "dbo", 3)]
    [InlineData("Orders", "test", 4)]
    public async Task ReadPreviewBatchAsync_fromTable_ShouldReturnData(string table, string schema, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();
        //_sut.Parameters[TableNameKey] = table;
        //_sut.Parameters[SchemaNameKey] = schema;


        DataSourceConfiguration sourceConfiguration = new()
        {
            Name = "DS1",
            TableOrSheet = $"{schema}.{table}"
        };
        var reader = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters, sourceConfiguration).Build();
        //Act
        var result = await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None);
        // Assert
        Assert.Equal(expected, result.Count());
    }

    [Theory]
    [InlineData("SELECT * FROM [dbo].[Users]", 3)]
    [InlineData("SELECT * FROM [test].[Orders]", 4)]
    public async Task ReadPreviewBatchAsync_ShouldReturnData(string query, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();
        //_sut.Parameters[QueryKey] = query;
        DataSourceConfiguration configuration = new()
        {
            Query = query,
            Name = "DataSource1"
        };
        var reader = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters, configuration).Build();
        //Act
        var result = await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None);
        // Assert
        Assert.Equal(expected, result.Count());
    }
    [Theory]
    [InlineData("SELECT * FROM [dbo].[Users]", 3)]
    [InlineData("SELECT * FROM [test].[Orders]", 4)]
    public void GetRowCount_WithCustomQuery_ShouldReturnCount(string query, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();

        //_sut.Parameters[QueryKey] = query;
        DataSourceConfiguration configuration = new()
        {
            Query = query,
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters, configuration).Build();
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

        var query = @"SELECT Id,SUM(Amount) Amount FROM [dbo].[Users]
                                        JOIN [test].[Orders] ON [test].[Orders].UserId = [dbo].[Users].Id
                                        GROUP BY Id";
        DataSourceConfiguration configuration = new()
        {
            Query = query,
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.SQLServer, _sut.Parameters, configuration).Build();
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

    public void Dispose()
    {
        // Cleanup: Drop test database
        using var connection = new SqlConnection(
            $"Server={TestServer};Database=master;Integrated Security=True");
        connection.Open();

        // Make sure there are no active connections to the test database
        var killSessionsCmd = $@"
                ALTER DATABASE {TestDatabase} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE IF EXISTS {TestDatabase};";

        var killLoginSessionsCmd = @"
                DECLARE @kill varchar(8000) = '';
                SELECT @kill = @kill + 'KILL ' + CONVERT(varchar(5), session_id) + ';'
                FROM sys.dm_exec_sessions
                WHERE login_name = 'sadmin'
                EXEC(@kill);";
        using var loginKill_command = new SqlCommand(killLoginSessionsCmd, connection);
        loginKill_command.ExecuteNonQuery();

        var deleteLoginCmd = @"
                IF EXISTS (SELECT * FROM sys.sql_logins WHERE name = 'sadmin')
                BEGIN
                    DROP LOGIN sadmin;
                END";
        using var user_command = new SqlCommand(deleteLoginCmd, connection);
        user_command.ExecuteNonQuery();

        using var command = new SqlCommand(killSessionsCmd, connection);
        command.ExecuteNonQuery();
    }

}
