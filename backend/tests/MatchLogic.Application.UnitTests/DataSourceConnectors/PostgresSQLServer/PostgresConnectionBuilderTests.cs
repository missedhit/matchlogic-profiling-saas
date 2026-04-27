using MatchLogic.Application.Features.Import;
using MatchLogic.Application.Interfaces.Import;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using MatchLogic.Infrastructure;
using LiteDB;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.PostgresSQLServer;
public class PostgresConnectionBuilderTests : IDisposable
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

    //private const string TableNameKey = "TableName";
    //private const string SchemaNameKey = "SchemaName";
    //private const string QueryKey = "Query";
    #endregion

    private readonly PostgresConnectionConfig _sut;
    private const string TestHost = "localhost";
    //private const string TestDatabase = "MatchLogictest";
    private const string TestUsername = "postgres";
    private const string TestPassword = "admin123";
    private const string TestPort = "5432";
    private readonly IConnectionBuilder connectionBuilder;
    private readonly IColumnFilter columnFilter;

    private readonly string TestDatabase;

    public PostgresConnectionBuilderTests()
    {
        _sut = new PostgresConnectionConfig();
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
        // Skip actual database setup for unit tests
        SetupTestDatabase().Wait();
    }

    private async Task SetupTestDatabase()
    {
        using var masterConnection = new NpgsqlConnection(
            $"host={TestHost};port={TestPort};database=postgres;username={TestUsername};password={TestPassword}");
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
        using var testConnection = new NpgsqlConnection($"host={TestHost};port={TestPort};database={TestDatabase.ToLower()};username={TestUsername};password={TestPassword}");
        //$"host=localhost;port=5432;database={TestDatabase.ToLower()};username=postgres;password=admin123");
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
            );";

        using var command = new NpgsqlCommand(createTableCmd, testConnection);
        await command.ExecuteNonQueryAsync();
        DemoData();
    }
    private void DemoData()
    {
        using var connection = new NpgsqlConnection(
            $"host={TestHost};port={TestPort};database={TestDatabase.ToLower()};username={TestUsername};password={TestPassword}");
        connection.Open();
        var usersQuery = @"INSERT INTO public.""Users"" (""Name"", ""Email"") VALUES
                                ('TestUser1', 'demo1@mail.com'),
                                ('TestUser2', 'demo2@mail.com'),
                                ('TestUser3', 'demo3@mail.com');";
        var ordersQuery = @"INSERT INTO test.""Orders"" (""UserId"", ""OrderDate"", ""Amount"") VALUES
                    (1, NOW(), 250),
                    (2, NOW(), 600),
                    (3, NOW(), 160),
                    (1, NOW(), 50);";

        using var user_command = new NpgsqlCommand(usersQuery, connection);
        user_command.ExecuteNonQuery();

        using var order_command = new NpgsqlCommand(ordersQuery, connection);
        order_command.ExecuteNonQuery();

    }
    [Fact]
    public async Task TestConnection_WithValidCredentials_ShouldReturnTrue()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();

        // Act
        var result = await conn.TestConnectionAsync();

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TestConnection_WithInvalidHost_ShouldReturnFalse()
    {
        // Arrange
        _sut.Parameters[ServerKey] = "invalidhost";
        _sut.Parameters[DatabaseKey] = TestDatabase;
        _sut.Parameters[AuthTypeKey] = "SQL";
        _sut.Parameters[UsernameKey] = TestUsername;
        _sut.Parameters[PasswordKey] = TestPassword;

        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();

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
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();

        // Act
        var tables = await conn.GetAvailableTables();

        // Assert
        Assert.NotNull(tables);
        Assert.Contains(tables, t => t.Name.ToLower() == "users" && t.Schema == "public");
    }

    [Theory]
    [InlineData("public.Users")]
    [InlineData("public.\"Users\"")]
    [InlineData("Users")]
    [InlineData(".Users")]
    public async Task GetTableSchema_ForUsersTable_ShouldReturnCorrectSchema(string tableName)
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();

        // Act
        var schema = await conn.GetTableSchema(tableName);

        // Assert
        Assert.NotNull(schema);
        Assert.NotNull(schema.Columns);
        // Add assertions for expected columns
        Assert.Equal(3, schema.Columns.Count);
        
        var idColumn = schema.Columns.First(c => c.Name == "Id");
        Assert.Equal("System.Int32", idColumn.DataType);
        Assert.False(idColumn.IsNullable);

        var nameColumn = schema.Columns.First(c => c.Name == "Name");
        Assert.Equal("System.String", nameColumn.DataType);  // Updated to match actual type with length
        Assert.False(nameColumn.IsNullable);

        var emailColumn = schema.Columns.First(c => c.Name == "Email");
        Assert.Equal("System.String", emailColumn.DataType); // Updated to match actual type with length
        Assert.False(emailColumn.IsNullable);

        //var createdDateColumn = schema.Columns.First(c => c.Name == "CreatedDate");
        //Assert.Equal("System.DateTime", createdDateColumn.DataType);
        //Assert.True(createdDateColumn.IsNullable);
    }
    [Theory]
    [InlineData("test.Orders")]
    [InlineData("test.\"Orders\"")]
    public async Task GetTableSchema_ForOrdersTable_WithSchemaPrefix_ShouldReturnCorrectSchema(string tableName)
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();
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
        Assert.False(amountColumn.IsNullable);
    }
    [Fact]
    public async Task GetTableSchema_WithNonexistentTable_ShouldReturnEmptySchema()
    {
        // Arrange
        SetupValidConnectionParameters();
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();
        // Act
        var schema = await conn.GetTableSchema("dbo.NonexistentTable");

        // Assert
        Assert.NotNull(schema);
        Assert.Empty(schema.Columns);
    }


    
    [Fact]
    public async Task GetAvailableTables_WithSQLAuth_ShouldReturnDatabases()
    {
        // Arrange
        SetupValidConnectionParameters();
       
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();
        // Act

        var schema = await (conn as IDataBaseConnectionReaderStrategy).GetAvailableDatabasesAsync();

        // Assert
        Assert.NotNull(schema);
        Assert.True(schema.Count > 0);
        Assert.Contains(TestDatabase, schema);
    }



    [Theory]
    [InlineData("SELECT \"Name\", \"Email\" FROM public.\"Users\";", 2)]
    [InlineData("SELECT \"OrderId\", \"UserId\", \"Amount\" FROM test.\"Orders\";", 3)]
    public void GetHeaders_WithCustomQuery_ShouldReturnHeaders(string query,int count)
    {
        // Arrange
        SetupValidConnectionParameters();

        DataSourceConfiguration configuration = new()
        {
            Query = query,
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters, configuration).Build();
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

        var reader = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters).Build();
        //Act
        var exception = await Assert.ThrowsAsync<ArgumentException>(
            async () => await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None));
        //Assert
        Assert.Equal("Either TableName or Query parameter must be provided.", exception.Message);
    }
    [Theory]
    [InlineData("Users", "public", 3)]
    [InlineData("Orders", "test", 4)]
    public async Task ReadPreviewBatchAsync_fromTable_ShouldReturnData(string table, string schema, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();

        DataSourceConfiguration sourceConfiguration = new()
        {
            Name = "DS1",
            TableOrSheet = $"{schema}.{table}"
        };
        var reader = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters, sourceConfiguration).Build();
        //Act
        var result = await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None);
        // Assert
        Assert.Equal(expected, result.Count());
    }

    [Theory]
    [InlineData("SELECT * FROM public.\"Users\"", 3)]
    [InlineData("SELECT * FROM test.\"Orders\"", 4)]
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
        var reader = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters, configuration).Build();
        //Act
        var result = await reader.ReadPreviewBatchAsync(new DataImportOptions() { PreviewLimit = 10 }, columnFilter, CancellationToken.None);
        // Assert
        Assert.Equal(expected, result.Count());
    }
    [Theory]
    [InlineData("SELECT * FROM public.\"Users\"", 3)]
    [InlineData("SELECT * FROM test.\"Orders\"", 4)]
    public void GetRowCount_WithCustomQuery_ShouldReturnCount(string query, int expected)
    {
        // Arrange
        SetupValidConnectionParameters();
        DataSourceConfiguration configuration = new()
        {
            Query = query,
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters, configuration).Build();
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

        var query = @"SELECT ""Id"",SUM(""Amount"") ""Amount"" FROM public.""Users""
                                        JOIN test.""Orders"" ON test.""Orders"".""UserId"" = public.""Users"".""Id""
                                        GROUP BY ""Id""";
        DataSourceConfiguration configuration = new()
        {
            Query = query,
            Name = "DataSource1"
        };
        var conn = connectionBuilder.WithArgs(DataSourceType.PostgreSQL, _sut.Parameters, configuration).Build();
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
    private void SetupValidConnectionParameters()
    {
        _sut.Parameters[ServerKey] = TestHost;
        _sut.Parameters[DatabaseKey] = TestDatabase;
        _sut.Parameters[AuthTypeKey] = "SQL";
        _sut.Parameters[UsernameKey] = TestUsername;
        _sut.Parameters[PasswordKey] = TestPassword;
    }
    public void Dispose()
    {
        using var connection = new NpgsqlConnection(
                $"host={TestHost};port={TestPort};database=postgres;username={TestUsername};password={TestPassword}");
        connection.Open();

        int dbExists = 0;
        using (var commanda = new NpgsqlCommand($@"SELECT 1 FROM pg_database WHERE datname = '{TestDatabase.ToLower()}'", connection))
        {
            var result = commanda.ExecuteScalar();
            dbExists = result != null ? (int)result : 0;
        }

        if (dbExists > 0)
        {
            // Terminate all connections to the test database
            var killSessionsCmd = $@"
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = '{TestDatabase.ToLower()}'
                      AND pid <> pg_backend_pid();";
            using var killCommand = new NpgsqlCommand(killSessionsCmd, connection);
            killCommand.ExecuteNonQuery();
            using var createCommand = new NpgsqlCommand($@"DROP DATABASE {TestDatabase.ToLower()}", connection);
            createCommand.ExecuteNonQuery();
        }
    }
}