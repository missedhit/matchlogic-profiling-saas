using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Preview.Columns;
using MatchLogic.Api.Handlers.DataSource.Preview.Data;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
using MatchLogic.Domain.Import;
using MySqlConnector;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource.SourceTypes;

[Collection("Data Import MySQL")]
public class DataImportMySQLCreateDataSourceTest : DataSourceTypeTest<BaseConnectionInfo>, IDisposable
{
    #region Keys
    private const string ServerKey = "Server";
    private const string PortKey = "Port";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string QueryKey = "Query";
    #endregion

    public override string TableName => "Users";
    
    // Test environment settings
    private const string TestHost = "localhost";
    private const string TestPort = "3306";
    private readonly string TestDatabase;
    private const string TestUsername = "root";
    private const string TestPassword = "admin123";
    
    public DataImportMySQLCreateDataSourceTest() : base(DataSourceType.MySQL)
    {
        TestDatabase = $"MatchLogictest_{Guid.NewGuid():N}";
        SetupTestDatabase().Wait();
    }

    private async Task SetupTestDatabase()
    {
        using var masterConnection = new MySqlConnection(
            $"Server={TestHost};Port={TestPort};Uid={TestUsername};Pwd={TestPassword}");
        await masterConnection.OpenAsync();

        // Create test database
        using (var createCommand = new MySqlCommand($"CREATE DATABASE IF NOT EXISTS `{TestDatabase}`", masterConnection))
        {
            await createCommand.ExecuteNonQueryAsync();
        }

        // Create test tables
        using var testConnection = new MySqlConnection(
            $"Server={TestHost};Port={TestPort};Database={TestDatabase};Uid={TestUsername};Pwd={TestPassword}");
        await testConnection.OpenAsync();

        var createTableCmd = @"
            CREATE TABLE IF NOT EXISTS `Users` (
                `Id` INT AUTO_INCREMENT PRIMARY KEY,
                `Name` VARCHAR(100),
                `Email` VARCHAR(255) NOT NULL
            );

            CREATE TABLE IF NOT EXISTS `Orders` (
                `OrderId` INT AUTO_INCREMENT PRIMARY KEY,
                `UserId` INT NOT NULL,
                `OrderDate` TIMESTAMP NOT NULL,
                `Amount` DECIMAL(18,2)
            );";

        using (var command = new MySqlCommand(createTableCmd, testConnection))
        {
            await command.ExecuteNonQueryAsync();
        }

        await InsertDemoData(testConnection);
    }

    private async Task InsertDemoData(MySqlConnection connection)
    {
        var usersQuery = @"INSERT INTO `Users` (`Name`, `Email`) VALUES
                            ('TestUser1', 'demo1@mail.com'),
                            ('TestUser2', 'demo2@mail.com'),
                            ('TestUser3', 'demo3@mail.com')";

        var ordersQuery = @"INSERT INTO `Orders` (`UserId`, `OrderDate`, `Amount`) VALUES
                            (1, NOW(), 250),
                            (2, NOW(), 600),
                            (3, NOW(), 160),
                            (1, NOW(), 50)";

        using (var command = new MySqlCommand(usersQuery, connection))
        {
            await command.ExecuteNonQueryAsync();
        }

        using (var command = new MySqlCommand(ordersQuery, connection))
        {
            await command.ExecuteNonQueryAsync();
        }
    }

    public void Dispose()
    {
        // Cleanup: Drop test database
        using var connection = new MySqlConnection(
            $"Server={TestHost};Port={TestPort};Uid={TestUsername};Pwd={TestPassword}");
        connection.Open();

        var dropDbCmd = $"DROP DATABASE IF EXISTS `{TestDatabase}`";
        using (var command = new MySqlCommand(dropDbCmd, connection))
        {
            command.ExecuteNonQuery();
        }
    }

    protected override BaseConnectionInfo CreateConnectionInfo()
    {
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };
        return CreateMySqlConnectionInfo(parameters);
    }

    private BaseConnectionInfo CreateMySqlConnectionInfo(Dictionary<string, string> parameters)
    {        
        return new BaseConnectionInfo
        {
            Type = DataSourceType.MySQL,
            Parameters = parameters
        };
    }

    protected override BaseConnectionInfo CreateInvalidConnectionInfo()
    {
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, "nonexistent_db" },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };
        return CreateMySqlConnectionInfo(parameters);
    }

    #region Create Data Source Tests

    [Fact]
    public async Task CreateDataSource_ValidCredentials_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };
        
        var connection = CreateMySqlConnectionInfo(parameters);
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: connection,
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: TableName, Query: null, ColumnMappings: []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task CreateDataSource_InvalidDatabase_ShouldReturn_Error(string database)
    {
        // Arrange
        Project testProject = await CreateTestProject();
        
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, database },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };
        
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            CreateMySqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: "Users", Query: null, ColumnMappings: []),
                new DataSourceRequest(Name: "DataSource2", TableName: "Orders", Query: null, ColumnMappings: [])
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            ValidationMessages.Required($"{DatabaseKey} parameter"),
        ]);
    }
    [Fact]
    public async Task CreateDataSource_WithQuery_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var connection = CreateConnectionInfo();
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: connection,
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: null, Query: "SELECT * FROM Users", ColumnMappings: []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task CreateDataSource_WithBothTableAndQuery_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var connection = CreateConnectionInfo();
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: connection,
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: TableName, Query: "SELECT * FROM Users", ColumnMappings: []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task CreateDataSource_EmptyName_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: CreateConnectionInfo(),
            dataSources:
            [
                new DataSourceRequest(Name: string.Empty, TableName: "Users", Query: null, ColumnMappings: [])
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "Name is required for each Data Source.",
        ]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("     ")]
    public async Task CreateDataSource_InvalidHost_ShouldReturn_Error(string host)
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, host },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            CreateMySqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: "Users", Query: null, ColumnMappings: [])
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            ValidationMessages.Required($"{ServerKey} parameter"),
        ]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("     ")]
    [InlineData("abc")]
    public async Task CreateDataSource_InvalidPort_ShouldReturn_Error(string port)
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, port },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            CreateMySqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: "Users", Query: null, ColumnMappings: [])
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        if (port == "abc")
        {
            AssertBaseValidationErrorsContainMessages(response,
            [
                ValidationMessages.Invalid($"{PortKey} parameter"),
            ]);
        }
        else
        {
            AssertBaseValidationErrorsContainMessages(response,
            [
                ValidationMessages.CannotContainEmptyOrWhitespace($"{PortKey} parameter"),
            ]);
        }
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("", "")]
    [InlineData("   ", "    ")]
    public async Task CreateDataSource_SingleDataSource_NullTableNameAndQuery_ShouldReturn_Error(string? tableName, string? query)
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: CreateConnectionInfo(),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: tableName, Query: query, ColumnMappings: []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "Either TableName or Query must be provided."
        ]);
    }
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("     ")]
    public async Task CreateDataSource_InvalidUsername_ShouldReturn_Error(string username)
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, username },
            { PasswordKey, TestPassword }
        };

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            CreateMySqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: TestDatabase+".users", Query: null, ColumnMappings: []),
                new DataSourceRequest(Name: "DataSource2", TableName: TestDatabase+".orders", Query: null, ColumnMappings: [])
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            //ValidationMessages.Required($"{UsernameKey} parameter"),
            $"{UsernameKey} and {PasswordKey} must be provided.",
        ]);
    }
    #endregion

    #region Preview Data Tests

    [Fact]
    public async Task DataImport_Preview_Data_WithDuplicateColumnNames_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword },
            { QueryKey, "SELECT Name, Email FROM Users" }
        };
        
        var connectionInfo = CreateMySqlConnectionInfo(parameters);
        var request = new PreviewDataRequest(
            TableName: TableName,
            Connection: connectionInfo,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.SameNameColumnsCount.Should().Be(0);
    }
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task DataImport_Preview_Data_NullEmptyTableName_ShouldReturn_Error(string? tableName)
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            TableName: tableName,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("", "Either TableName or Query parameter must be provided."),
        ]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task DataImport_Preview_Data_NullEmptyQuery_ShouldReturn_Error(string? query)
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            TableName: null,
            Query: query,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("", "Either TableName or Query parameter must be provided."),
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Data_InvalidConnectionString_ShouldReturn_Error()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, "invalid-host" },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var connectionInfo = CreateMySqlConnectionInfo(parameters);
        var request = new PreviewDataRequest(
            TableName: TableName,
            Connection: connectionInfo,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        response.Should().NotBeNull();
        response.IsSuccess.Should().BeFalse();
        response.Status.Should().Be(ResultStatus.Error);
    }
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Data_InvalidHost_ShouldReturn_Error(string host)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, host },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword },
        };

        var connectionInfo = CreateMySqlConnectionInfo(parameters);
        var request = new PreviewDataRequest(
            TableName: TableName,
            Connection: connectionInfo,
            ColumnMappings: new Dictionary<string, ColumnMapping>()
            {
                { "name", new ColumnMapping(){
                                SourceColumn = "name",
                                TargetColumn = "Name1",
                                Include = true
                                }
                },
                { "email", new ColumnMapping()
                                {
                                SourceColumn = "email",
                                TargetColumn = "Email1",
                                Include = true
                                }
                }
            });

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
          ValidationMessages.Required($"{ServerKey} parameter"),
        ]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Data_InvalidDatabase_ShouldReturn_Error(string database)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, database },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var connectionInfo = CreateMySqlConnectionInfo(parameters);
        var request = new PreviewDataRequest(
            TableName: TableName,
            Connection: connectionInfo,
            ColumnMappings: new Dictionary<string, ColumnMapping>()
            {
                { "name", new ColumnMapping(){
                                SourceColumn = "name",
                                TargetColumn = "Name1",
                                Include = true
                                }
                },
                { "email", new ColumnMapping()
                                {
                                SourceColumn = "email",
                                TargetColumn = "Email1",
                                Include = true
                                }
                }
            });

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            ValidationMessages.Required($"{DatabaseKey} parameter"),
        ]);
    }

    #endregion

    #region Preview Columns
    [Fact]
    public async Task DataImport_Preview_Columns_CustomQuery_ShouldReturn_Success()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateMySqlConnectionInfo(parameters);

        // Act
        var response = await SendRequestPreviewColumnsResponse(request);

        // Assert
        List<TableInfo> assertColumns = new()
        {           
            new TableInfo { Name = "orders", Schema = TestDatabase, Type = "TABLE"},
            new TableInfo { Name = "users", Schema = TestDatabase, Type = "TABLE" },
        };

        AssertColumnsSuccessResponse(response, assertColumns);

        response.Value.Metadata.Tables[1].Columns[0].Name.ToLower().Should().Be("id");
        response.Value.Metadata.Tables[1].Columns[1].Name.ToLower().Should().Be("name");
        response.Value.Metadata.Tables[1].Columns[2].Name.ToLower().Should().Be("email");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Columns_NullHost_ShouldReturn_Error(string host)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, host },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateMySqlConnectionInfo(parameters);

        // Act
        var response = await SendRequestPreviewColumnsResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
           ValidationMessages.Required($"{ServerKey} parameter"),
        ]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Columns_InvalidDatabase_ShouldReturn_Error(string database)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, database },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateMySqlConnectionInfo(parameters);

        // Act
        var response = await SendRequestPreviewColumnsResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
           ValidationMessages.Required($"{DatabaseKey} parameter"),
        ]);
    }
    #endregion

    #region Preview Tables
    //[Fact]
    //public async Task DataImport_Preview_Tables_WithValidConnection_ShouldReturn_Success()
    //{
    //    // Arrange
    //    var request = CreateConnectionInfo();

    //    // Act
    //    var response = await SendRequestPreviewTablesResponse(request);

    //    // Assert
    //    AssertBaseSuccessResponse(response);
    //    response.Value.Tables.Count.Should().BeGreaterThan(0);
    //}

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Tables_NullHost_ShouldReturn_Error(string host)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, host },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateMySqlConnectionInfo(parameters);

        // Act
        var response = await SendRequestPreviewTablesResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
           ValidationMessages.Required($"{ServerKey} parameter"),
        ]);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Tables_InvalidDatabase_ShouldReturn_Error(string database)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, database },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateMySqlConnectionInfo(parameters);

        // Act
        var response = await SendRequestPreviewTablesResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
           ValidationMessages.Required($"{DatabaseKey} parameter"),
        ]);
    }
    #endregion

    #region Table Name Tests
    [Fact]
    public async Task CreateDataSource_SingleDataSource_EmptyTableName_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: CreateConnectionInfo(),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: String.Empty, []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "DataSources[0]", "Either TableName or Query must be provided.");
    }

    [Fact]
    public async Task CreateDataSource_MultipleDataSource_EmptyTableName_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: CreateConnectionInfo(),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: String.Empty, []),
                new DataSourceRequest(Name: "DataSource2", TableName: String.Empty, [])
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("DataSources[0]", "Either TableName or Query must be provided."),
        ]);
    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_TableName151CharactersLong_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var tableName = new string('A', Application.Common.Constants.FieldLength.NameMaxLength + 1); // 151 Characters Long
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: CreateConnectionInfo(),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: tableName, []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("DataSources[0].TableName", ValidationMessages.MaxLength("Table name", Application.Common.Constants.FieldLength.NameMaxLength)),
        ]);
    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_TableName150CharactersLong_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var tableName = new string('A', Application.Common.Constants.FieldLength.NameMaxLength); // 150 Characters Long
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: CreateConnectionInfo(),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: tableName, []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }
    #endregion

    #region Available Databases Tests

    [Fact]
    public async Task DataImport_AvailableDatabases_ShouldReturn_Success()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };
        
        var request = CreateMySqlConnectionInfo(parameters);
        
        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);
        
        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Count.Should().BeGreaterThan(0);
        response.Value.Should().Contain(TestDatabase);
        response.Value.Should().Contain("mysql"); // System database
    }
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_AvailableDatabases_InvalidHost_ShouldReturn_Error(string host)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, host },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateMySqlConnectionInfo(parameters);

        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);

        // Assert
        AssertBaseInvalidResponse(response);
    }

    [Fact]
    public async Task DataImport_AvailableDatabases_InaccessibleHost_ShouldReturn_Error()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, "invalid-host" },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };

        var request = CreateMySqlConnectionInfo(parameters);

        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);

        // Assert
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.Error);
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().BeNullOrEmpty();
        response.Errors.Should().NotBeEmpty();
    }

    #endregion

    #region Test Connection Tests

    [Fact]
    public async Task DataImport_TestConnection_WithInvalidCredentials_ShouldReturn_Error()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, "wrong_password" }
        };
        
        var request = CreateMySqlConnectionInfo(parameters);
        
        // Act
        var response = await SendRequestTestConnectionResponse(request);

        // Assert
        AssertBaseErrorMessage(response, "Connection failed");
    }

    #endregion

    protected void AssertColumnsSuccessResponse(Result<PreviewColumnsResponse> response, List<TableInfo> tableInfos)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Metadata.Should().NotBeNull();

        response.Value.Metadata.Tables.Should().NotBeEmpty();
        response.Value.Metadata.Tables.Should().HaveCountGreaterThan(1);
        response.Value.Metadata.Tables.Should().HaveCount(tableInfos.Count);

        response.Value.Metadata.ColumnMappings.Should().BeEmpty();

        for (int i = 0; i < tableInfos.Count; i++)
        {
            response.Value.Metadata.Tables[i].Name.ToLower().Should().Be(tableInfos[i].Name);
            response.Value.Metadata.Tables[i].Schema.Should().Be(tableInfos[i].Schema);
            response.Value.Metadata.Tables[i].Type.Should().Be(tableInfos[i].Type);
            response.Value.Metadata.Tables[i].Columns.Should().HaveCountGreaterThan(1);
        }
    }

}