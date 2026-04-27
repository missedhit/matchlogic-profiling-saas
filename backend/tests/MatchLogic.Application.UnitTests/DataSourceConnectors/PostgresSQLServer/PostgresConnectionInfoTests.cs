using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.PostgresSQLServer;

public class PostgresConnectionInfoTests 
{
    #region Keys
    private const string ServerKey = "Server";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string AuthTypeKey = "AuthType";
    private const string PortKey = "Port";
    private const string ConnectionTimeoutKey = "ConnectionTimeout";

    #endregion

    private readonly PostgresConnectionConfig _sut;
    private const string TestHost = "localhost";
    private const string TestUsername = "postgres";
    private const string TestPassword = "admin123";
    private const string TestPort = "5432";
    private const string TestDatabase = "MatchLogictest_1";

    public PostgresConnectionInfoTests()
    {
        _sut = new PostgresConnectionConfig();
    }
    [Fact]
    public void Type_ShouldReturn_PostgreSQLDataSourceType()
    {
        // Arrange
        // Act
        var result = _sut.CanCreateFromArgs(DataSourceType.PostgreSQL);
        // Assert
        Assert.True(result);
    }

    [Fact]
    public void Type_ShouldNotMatch_OtherDataSourceTypes()
    {
        // Arrange
        // Act
        var result = _sut.CanCreateFromArgs(DataSourceType.SQLServer);
        // Assert
        Assert.False(result);
    }

    [Fact]
    public void CreateFromArgs_WithInvalidType_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

        Assert.Equal("Invalid data source type for PostgresConnectionConfig", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithValidParameters_ShouldCreateConfigObject()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        // Act
        var config = _sut.CreateFromArgs(DataSourceType.PostgreSQL, args);

        // Assert
        Assert.NotNull(config);
        Assert.IsType<PostgresConnectionConfig>(config);
        Assert.Equal(args, config.Parameters);
    }

    [Fact]
    public void CreateFromArgs_WithMissingHost_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("Server is required for PostgreSQL connection", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithEmptyHost_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, "" },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("Server is required for PostgreSQL connection", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithMissingAuthType_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("AuthType is required for PostgreSQL connection", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithEmptyAuthType_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("AuthType is required for PostgreSQL connection", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithSqlAuth_MissingUsername_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { PasswordKey, TestPassword }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("Username is required for SQL Authentication", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithSqlAuth_EmptyUsername_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, "" },
            { PasswordKey, TestPassword }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("Username is required for SQL Authentication", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithSqlAuth_MissingPassword_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("Password is required for SQL Authentication", exception.Message);
    }

    [Fact]
    public void CreateFromArgs_WithSqlAuth_EmptyPassword_ShouldThrowArgumentException()
    {
        // Arrange
        var args = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, "" }
        };

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(
            () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

        Assert.Equal("Password is required for SQL Authentication", exception.Message);
    }

    [Fact]
    public void ConnectionString_WithSqlAuth_ShouldBuildCorrectly()
    {
        // Arrange
        _sut.Parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var connectionString = postgresConfig.ConnectionString;

        // Assert
        Assert.Contains($"Host={TestHost};", connectionString);
        Assert.Contains($"Port={TestPort};", connectionString);
        Assert.Contains($"Database={TestDatabase};", connectionString);
        Assert.Contains($"Username={TestUsername};", connectionString);
        Assert.Contains($"Password={TestPassword};", connectionString);
    }

    [Fact]
    public void ConnectionString_WithDefaultPort_ShouldUseDefaultPort()
    {
        // Arrange
        _sut.Parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var connectionString = postgresConfig.ConnectionString;

        // Assert
        Assert.Contains("Port=5432;", connectionString);
    }

    [Fact]
    public void ConnectionString_WithInvalidPort_ShouldUseDefaultPort()
    {
        // Arrange
        _sut.Parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, "notanumber" },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var connectionString = postgresConfig.ConnectionString;

        // Assert
        Assert.Contains("Port=5432;", connectionString);
    }

    

    [Fact]
    public void Query_WithNullSourceConfig_ShouldReturnEmptyString()
    {
        // Arrange
        _sut.SourceConfig = null;
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var query = postgresConfig.Query;

        // Assert
        Assert.Equal(string.Empty, query);
    }

    [Fact]
    public void Query_WithNullQueryInSourceConfig_ShouldReturnEmptyString()
    {
        // Arrange
        _sut.SourceConfig = new DataSourceConfiguration
        {
            Query = null
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var query = postgresConfig.Query;

        // Assert
        Assert.Equal(string.Empty, query);
    }

    [Fact]
    public void Query_WithValidQuery_ShouldReturnQuery()
    {
        // Arrange
        const string testQuery = "SELECT * FROM users";
        _sut.SourceConfig = new DataSourceConfiguration
        {
            Query = testQuery
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var query = postgresConfig.Query;

        // Assert
        Assert.Equal(testQuery, query);
    }

    [Fact]
    public void TableName_WithNullSourceConfig_ShouldReturnEmptyString()
    {
        // Arrange
        _sut.SourceConfig = null;
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var tableName = postgresConfig.TableName;

        // Assert
        Assert.Equal(string.Empty, tableName);
    }

    [Fact]
    public void TableName_WithNullTableOrSheet_ShouldReturnEmptyString()
    {
        // Arrange
        _sut.SourceConfig = new DataSourceConfiguration
        {
            TableOrSheet = null
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var tableName = postgresConfig.TableName;

        // Assert
        Assert.Equal(string.Empty, tableName);
    }

    [Fact]
    public void TableName_WithSchemaPrefix_ShouldReturnOnlyTableName()
    {
        // Arrange
        _sut.SourceConfig = new DataSourceConfiguration
        {
            TableOrSheet = "public.users"
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var tableName = postgresConfig.TableName;

        // Assert
        Assert.Equal("users", tableName);
    }

    [Fact]
    public void TableName_WithoutSchemaPrefix_ShouldReturnTableName()
    {
        // Arrange
        _sut.SourceConfig = new DataSourceConfiguration
        {
            TableOrSheet = "users"
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var tableName = postgresConfig.TableName;

        // Assert
        Assert.Equal("users", tableName);
    }

    [Fact]
    public void SchemaName_WithNullSourceConfig_ShouldReturnDefault()
    {
        // Arrange
        _sut.SourceConfig = null;
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var schemaName = postgresConfig.SchemaName;

        // Assert
        Assert.Equal("public", schemaName);
    }

    [Fact]
    public void SchemaName_WithNullTableOrSheet_ShouldReturnDefault()
    {
        // Arrange
        _sut.SourceConfig = new DataSourceConfiguration
        {
            TableOrSheet = null
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var schemaName = postgresConfig.SchemaName;

        // Assert
        Assert.Equal("public", schemaName);
    }

    [Fact]
    public void SchemaName_WithSchemaPrefix_ShouldReturnSchemaName()
    {
        // Arrange
        _sut.SourceConfig = new DataSourceConfiguration
        {
            TableOrSheet = "custom.users"
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var schemaName = postgresConfig.SchemaName;

        // Assert
        Assert.Equal("custom", schemaName);
    }

    [Fact]
    public void SchemaName_WithoutSchemaPrefix_ShouldReturnDefaultSchema()
    {
        // Arrange
        _sut.SourceConfig = new DataSourceConfiguration
        {
            TableOrSheet = "users"
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var schemaName = postgresConfig.SchemaName;

        // Assert
        Assert.Equal("public", schemaName);
    }

    [Fact]
    public void ConnectionTimeout_WithValidTimeout_ShouldReturnCorrectTimeSpan()
    {
        // Arrange
        _sut.Parameters = new Dictionary<string, string>
        {
            { ConnectionTimeoutKey, "60" }
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var timeout = postgresConfig.ConnectionTimeout;

        // Assert
        Assert.Equal(TimeSpan.FromSeconds(60), timeout);
    }

    [Fact]
    public void ConnectionTimeout_WithInvalidTimeout_ShouldReturnDefaultTimeSpan()
    {
        // Arrange
        _sut.Parameters = new Dictionary<string, string>
        {
            { ConnectionTimeoutKey, "notanumber" }
        };
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var timeout = postgresConfig.ConnectionTimeout;

        // Assert
        Assert.Equal(TimeSpan.FromMinutes(30), timeout);
    }

    [Fact]
    public void ConnectionTimeout_WithNoTimeoutSpecified_ShouldReturnDefaultTimeSpan()
    {
        // Arrange
        _sut.Parameters = new Dictionary<string, string>();
        var postgresConfig = _sut as IDBConnectionInfo;

        // Act
        var timeout = postgresConfig.ConnectionTimeout;

        // Assert
        Assert.Equal(TimeSpan.FromMinutes(30), timeout);
    }

    [Theory]
    [InlineData("SELECT * FROM \"Users\"", true)]
    [InlineData("SELECT * FROM public.\"Users\"", true)]
    [InlineData("SELECT * FROM \"orders\"; DROP TABLE orders;", false)]
    [InlineData("INSERT INTO \"Users\" (id, name) VALUES (1, 'A')", false)]
    [InlineData("SELECT * FROM \"Users\" -- DROP TABLE users", false)]
    [InlineData("SELECT * FROM \"Users\"; SELECT * FROM \"orders\"", false)]
    [InlineData(@"SELECT ""Id"",SUM(""Amount"") ""Amount"" FROM public.""Users"" JOIN test.""Orders"" ON test.""Orders"".""UserId"" = public.""Users"".""Id"" GROUP BY ""Id""", true)]
    public void IsValidSelectQuery_ShouldValidateCorrectly(string query, bool expected)
    {
        // Arrange
        DataSourceConfiguration sourceConfiguration = new()
        {
            Name = "DS1",
            Query = query
        };
        _sut.SourceConfig = sourceConfiguration;
        //Act
        var result = _sut.IsValidSelectQuery();
        // Assert
        Assert.Equal(expected, result);
    }

}