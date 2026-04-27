using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;
using NPOI.SS.Formula.Functions;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.MSSQLServer
{
    public class SqlServerConnectionInfoTests
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

        private const string TestUsername = "sadmin";
        private const string TestPassword = "admin123";
        //private const string TestPort = "5432";


        private readonly SQLServerConnectionConfig _sut;
        private const string TestServer = "(localdb)\\MSSQLLocalDB"; // Using LocalDB for tests
        private readonly string TestDatabase = "MatchLogicTest";
        public SqlServerConnectionInfoTests()
        {
            _sut = new SQLServerConnectionConfig();
        }

        [Fact]
        public void Type_ShouldReturn_SqlServerDataSourceType()
        {
            // Arrange
            // Act
            var result = _sut.CanCreateFromArgs(DataSourceType.SQLServer);
            // Assert
            Assert.True(result);
        }

        [Fact]
        public void ValidateConnection_WithAllRequiredParameters_ShouldReturnTrue()
        {
            // Arrange

            _sut.Parameters["Server"] = "localhost";
            _sut.Parameters["Database"] = "TestDB";
            _sut.Parameters["AuthType"] = "Windows";
            _sut.Parameters["TableName"] = "DemoTable";
            // Act
            var result = _sut.ValidateConnection();

            // Assert
            Assert.True(result);
        }

        [Theory]
        [InlineData(null, "TestDB")]
        [InlineData("", "TestDB")]
        [InlineData("localhost", null)]
        [InlineData("localhost", "")]
        public void ValidateConnection_WithMissingRequiredParameters_ShouldReturnFalse(string server, string database)
        {
            // Arrange
            _sut.Parameters["Server"] = server;
            _sut.Parameters["Database"] = database;
            // Act

            var result = _sut.ValidateConnection();

            // Assert
            Assert.False(result);
        }

        [Fact]
        public void ConnectionString_WithWindowsAuth_ShouldBuildCorrectly()
        {
            // Arrange
            _sut.Parameters["Server"] = "localhost";
            _sut.Parameters["Database"] = "TestDB";
            _sut.Parameters["AuthType"] = "Windows";
            var sqlConfig = _sut as IDBConnectionInfo;
            // Act
            var connectionString = sqlConfig.ConnectionString;


            // Assert
            Assert.Contains("Integrated Security=True", connectionString);
            Assert.Contains("Data Source=localhost", connectionString);
            Assert.Contains("Initial Catalog=TestDB", connectionString);
        }

        [Fact]
        public void ConnectionString_WithSqlAuth_ShouldBuildCorrectly()
        {
            // Arrange
            _sut.Parameters["Server"] = TestServer;
            _sut.Parameters["Database"] = TestDatabase;
            _sut.Parameters["AuthType"] = "SQL";
            _sut.Parameters["Username"] = TestUsername;
            _sut.Parameters["Password"] = TestPassword;

            var sqlConfig = _sut as IDBConnectionInfo;

            // Act
            var connectionString = sqlConfig.ConnectionString;

            // Assert
            Assert.Contains($"User ID={TestUsername}", connectionString);
            Assert.Contains($"Password={TestPassword}", connectionString);
            Assert.Contains($"Data Source={TestServer}", connectionString);
            Assert.Contains($"Initial Catalog={TestDatabase}", connectionString);
        }

        [Fact]
        public void CreateFromArgs_WithInvalidType_ShouldThrowArgumentException()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "SQL" },
                { UsernameKey, TestUsername },
                { PasswordKey, TestPassword }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(
                () => _sut.CreateFromArgs(DataSourceType.PostgreSQL, args));

            Assert.Equal("Invalid data source type for SQLServerConnectionConfig", exception.Message);
        }

        [Fact]
        public void CreateFromArgs_WithValidParameters_ShouldCreateConfigObject()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "SQL" },
                { UsernameKey, TestUsername },
                { PasswordKey, TestPassword }
            };

            // Act
            var config = _sut.CreateFromArgs(DataSourceType.SQLServer, args);

            // Assert
            Assert.NotNull(config);
            Assert.IsType<SQLServerConnectionConfig>(config);
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
                () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

            Assert.Equal("Server is required for SQL Server connection", exception.Message);
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
                () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

            Assert.Equal("Server is required for SQL Server connection", exception.Message);
        }

        [Fact]
        public void CreateFromArgs_WithMissingAuthType_ShouldThrowArgumentException()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { UsernameKey, TestUsername },
                { PasswordKey, TestPassword }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(
                () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

            Assert.Equal("AuthType is required for SQL Server connection", exception.Message);
        }

        [Fact]
        public void CreateFromArgs_WithEmptyAuthType_ShouldThrowArgumentException()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "" },
                { UsernameKey, TestUsername },
                { PasswordKey, TestPassword }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(
                () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

            Assert.Equal("AuthType is required for SQL Server connection", exception.Message);
        }

        [Fact]
        public void CreateFromArgs_WithSqlAuth_MissingUsername_ShouldThrowArgumentException()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "SQL" },
                { PasswordKey, TestPassword }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(
                () => _sut.CreateFromArgs(DataSourceType.   SQLServer, args));

            Assert.Equal("Username is required for SQL Authentication", exception.Message);
        }

        [Fact]
        public void CreateFromArgs_WithSqlAuth_EmptyUsername_ShouldThrowArgumentException()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "SQL" },
                { UsernameKey, "" },
                { PasswordKey, TestPassword }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(
                () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

            Assert.Equal("Username is required for SQL Authentication", exception.Message);
        }

        [Fact]
        public void CreateFromArgs_WithSqlAuth_MissingPassword_ShouldThrowArgumentException()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "SQL" },
                { UsernameKey, TestUsername }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(
                () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

            Assert.Equal("Password is required for SQL Authentication", exception.Message);
        }

        [Fact]
        public void CreateFromArgs_WithSqlAuth_EmptyPassword_ShouldThrowArgumentException()
        {
            // Arrange
            var args = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "SQL" },
                { UsernameKey, TestUsername },
                { PasswordKey, "" }
            };

            // Act & Assert
            var exception = Assert.Throws<ArgumentException>(
                () => _sut.CreateFromArgs(DataSourceType.SQLServer, args));

            Assert.Equal("Password is required for SQL Authentication", exception.Message);
        }
        [Fact]
        public void ConnectionString_WithIntegratedAuth_ShouldBuildCorrectly()
        {
            // Arrange
            _sut.Parameters = new Dictionary<string, string>
            {
                { ServerKey, TestServer },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "Windows" },
            };

            var postgresConfig = _sut as IDBConnectionInfo;

            // Act
            var connectionString = postgresConfig.ConnectionString;

            // Assert
            Assert.Contains($"Data Source={TestServer};", connectionString);
            Assert.Contains("Integrated Security=True;", connectionString);
            Assert.Contains($"Initial Catalog={TestDatabase};", connectionString);
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
                TableOrSheet = "dbo.users"
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
            var dbConfig = _sut as IDBConnectionInfo;

            // Act
            var schemaName = dbConfig.SchemaName;

            // Assert
            Assert.Equal("", schemaName);
        }

        [Fact]
        public void SchemaName_WithNullTableOrSheet_ShouldReturnDefault()
        {
            // Arrange
            _sut.SourceConfig = new DataSourceConfiguration
            {
                TableOrSheet = null
            };
            var dbConfig = _sut as IDBConnectionInfo;

            // Act
            var schemaName = dbConfig.SchemaName;

            // Assert
            Assert.Equal("", schemaName);
        }

        [Fact]
        public void SchemaName_WithSchemaPrefix_ShouldReturnSchemaName()
        {
            // Arrange
            _sut.SourceConfig = new DataSourceConfiguration
            {
                TableOrSheet = "custom.users"
            };
            var dbConfig = _sut as IDBConnectionInfo;

            // Act
            var schemaName = dbConfig.SchemaName;

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
            var dbConfig = _sut as IDBConnectionInfo;

            // Act
            var schemaName = dbConfig.SchemaName;

            // Assert
            Assert.Equal("", schemaName);
        }

        [Fact]
        public void ConnectionTimeout_WithValidTimeout_ShouldReturnCorrectTimeSpan()
        {
            // Arrange
            _sut.Parameters = new Dictionary<string, string>
            {
                { ConnectionTimeoutKey, "60" }
            };
            var dbConfig = _sut as IDBConnectionInfo;

            // Act
            var timeout = dbConfig.ConnectionTimeout;

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
            var dbConfig = _sut as IDBConnectionInfo;

            // Act
            var timeout = dbConfig.ConnectionTimeout;

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(30), timeout);
        }

        [Fact]
        public void ConnectionTimeout_WithNoTimeoutSpecified_ShouldReturnDefaultTimeSpan()
        {
            // Arrange
            _sut.Parameters = new Dictionary<string, string>();
            var dbConfig = _sut as IDBConnectionInfo;

            // Act
            var timeout = dbConfig.ConnectionTimeout;

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(30), timeout);
        }

        [Theory]
        [InlineData("SELECT * FROM Users", true)]
        [InlineData("SELECT * FROM [dbo].[Users]", true)]
        [InlineData("SELECT * FROM orders; DROP TABLE orders;", false)]
        [InlineData("INSERT INTO users (id, name) VALUES (1, 'A')", false)]
        [InlineData("SELECT * FROM users -- DROP TABLE users", false)]
        [InlineData("SELECT * FROM users; SELECT * FROM orders", false)]
        [InlineData(@"SELECT Id,SUM(Amount) Amount FROM [dbo].[Users] JOIN [test].[Orders] ON [test].[Orders].UserId = [dbo].[Users].Id GROUP BY Id", true)]
        public void IsValidSelectQuery_ShouldValidateCorrectly(string query, bool expected)
        {
            // Arrange
            SetupValidConnectionParameters();
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


        /*[Fact]
        public async Task BuildQuery_WithBasicOptions_ShouldBuildCorrectQuery()
        {
            // Arrange
            var options = new QueryOptions
            {
                TableName = "Users",
                SelectedColumns = new List<string> { "Id", "Name", "Email" },

            };

            // Act
            var query = await _sut.BuildQuery(options);

            // Assert
            Assert.Equal("SELECT Id, Name, Email FROM Users", query);
        }

        [Fact]
        public async Task BuildQuery_WithAllOptions_ShouldBuildCorrectQuery()
        {
            // Arrange
            var options = new QueryOptions
            {
                TableName = "Users",
                SelectedColumns = new List<string> { "Id", "Name" },
                WhereClause = "Age > 18",
                OrderBy = "Name ASC"
            };

            // Act
            var query = await _sut.BuildQuery(options);

            // Assert
            Assert.Equal("SELECT Id, Name FROM Users WHERE Age > 18 ORDER BY Name ASC", query);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public async Task BuildQuery_WithInvalidTableName_ShouldThrowArgumentException(string tableName)
        {
            // Arrange
            var options = new QueryOptions { TableName = tableName };

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await _sut.BuildQuery(options));
        }

        [Fact]
        public async Task BuildQuery_WithNullOptions_ShouldThrowArgumentNullException()
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(
                async () => await _sut.BuildQuery(null));
        }*/



        private void SetupValidConnectionParameters()
        {
            _sut.Parameters["Server"] = TestServer;
            _sut.Parameters["Database"] = TestDatabase;
            _sut.Parameters["AuthType"] = "Windows";
        }


    }
}
