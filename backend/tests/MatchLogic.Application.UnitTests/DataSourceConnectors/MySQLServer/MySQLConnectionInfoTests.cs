using MatchLogic.Domain.Import;
using MatchLogic.Domain.Project;

namespace MatchLogic.Application.UnitTests.DataSourceConnectors.MySQLServer
{
    public class MySQLConnectionInfoTests 
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
        private const string TestDatabase = "MatchLogictest_1";
        private const string TestHost = "localhost";        
        private const string TestUsername = "root";
        private const string TestPassword = "admin123";
        private const string TestPort = "3306";

        public MySQLConnectionInfoTests()
        {
            _sut = new MySQLConnectionConfig();
            
        }
        [Fact]
        public void Type_ShouldReturn_MySQLDataSourceType()
        {
            // Arrange & Act
            var result = _sut.CanCreateFromArgs(DataSourceType.MySQL);
            
            // Assert
            Assert.True(result);
        }

        [Fact]
        public void Type_ShouldNotMatch_OtherDataSourceTypes()
        {
            // Arrange & Act
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

            Assert.Equal("Invalid data source type for MySQLConnectionConfig", exception.Message);
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
            var config = _sut.CreateFromArgs(DataSourceType.MySQL, args);

            // Assert
            Assert.NotNull(config);
            Assert.IsType<MySQLConnectionConfig>(config);
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
                () => _sut.CreateFromArgs(DataSourceType.MySQL, args));

            Assert.Equal("Server is required for MySQL connection", exception.Message);
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
                () => _sut.CreateFromArgs(DataSourceType.MySQL, args));

            Assert.Equal("Server is required for MySQL connection", exception.Message);
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
                () => _sut.CreateFromArgs(DataSourceType.MySQL, args));

            Assert.Equal("AuthType is required for MySQL connection", exception.Message);
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
                () => _sut.CreateFromArgs(DataSourceType.MySQL, args));

            Assert.Equal("AuthType is required for MySQL connection", exception.Message);
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

            // Act
            var connectionString = (_sut as IDBConnectionInfo).ConnectionString;

            // Assert
            Assert.Contains($"Server={TestHost}", connectionString);
            Assert.Contains($"Port={TestPort}", connectionString);
            Assert.Contains($"Database={TestDatabase}", connectionString);
            Assert.Contains($"User ID={TestUsername}", connectionString);
            Assert.Contains($"Password={TestPassword}", connectionString);
        }

        [Fact]
        public void ConnectionString_WithTrustServerCertificate_ShouldIncludeSslMode()
        {
            // Arrange
            _sut.Parameters = new Dictionary<string, string>
            {
                { ServerKey, TestHost },
                { DatabaseKey, TestDatabase },
                { AuthTypeKey, "SQL" },
                { UsernameKey, TestUsername },
                { PasswordKey, TestPassword },
                { TrustServerCertificateKey, "true" }
            };

            // Act
            var connectionString = (_sut as IDBConnectionInfo).ConnectionString;

            // Assert
            Assert.Contains("SslMode=Required", connectionString);
        }

        [Fact]
        public void Query_WithNullSourceConfig_ShouldReturnEmptyString()
        {
            // Arrange
            _sut.SourceConfig = null;
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var query = mysqlConfig.Query;

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
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var query = mysqlConfig.Query;

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
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var query = mysqlConfig.Query;

            // Assert
            Assert.Equal(testQuery, query);
        }

        [Fact]
        public void TableName_WithNullSourceConfig_ShouldReturnEmptyString()
        {
            // Arrange
            _sut.SourceConfig = null;
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var tableName = mysqlConfig.TableName;

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
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var tableName = mysqlConfig.TableName;

            // Assert
            Assert.Equal(string.Empty, tableName);
        }

        [Fact]
        public void TableName_WithValidTableName_ShouldReturnTableName()
        {
            // Arrange
            _sut.SourceConfig = new DataSourceConfiguration
            {
                TableOrSheet = "users"
            };
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var tableName = mysqlConfig.TableName;

            // Assert
            Assert.Equal("users", tableName);
        }

        [Fact]
        public void SchemaName_ShouldReturnEmptyString()
        {
            // Arrange
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var schemaName = mysqlConfig.SchemaName;

            // Assert
            Assert.Equal(string.Empty, schemaName);
        }

        [Fact]
        public void ConnectionTimeout_WithValidTimeout_ShouldReturnCorrectTimeSpan()
        {
            // Arrange
            _sut.Parameters = new Dictionary<string, string>
            {
                { ConnectionTimeoutKey, "60" }
            };
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var timeout = mysqlConfig.ConnectionTimeout;

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
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var timeout = mysqlConfig.ConnectionTimeout;

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(30), timeout);
        }

        [Fact]
        public void ConnectionTimeout_WithNoTimeoutSpecified_ShouldReturnDefaultTimeSpan()
        {
            // Arrange
            _sut.Parameters = new Dictionary<string, string>();
            var mysqlConfig = _sut as IDBConnectionInfo;

            // Act
            var timeout = mysqlConfig.ConnectionTimeout;

            // Assert
            Assert.Equal(TimeSpan.FromMinutes(30), timeout);
        }
    }
}