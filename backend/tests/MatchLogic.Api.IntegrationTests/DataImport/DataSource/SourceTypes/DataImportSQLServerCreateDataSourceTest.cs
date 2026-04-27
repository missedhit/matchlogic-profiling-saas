using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Preview.Columns;
using MatchLogic.Api.Handlers.DataSource.Preview.Data;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
using MatchLogic.Domain.Import;
using Microsoft.Data.SqlClient;
using System.Globalization;
using ColumnMapping = MatchLogic.Domain.Project.ColumnMapping;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource.SourceTypes;
[Collection("Data Import Sql Server")]
public class DataImportSQLServerCreateDataSourceTest : DataSourceTypeTest<BaseConnectionInfo>
{
    #region  Keys
    private const string ServerKey = "Server";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string AuthTypeKey = "AuthType";

    //private const string TableNameKey = "TableName";
    //private const string SchemaNameKey = "SchemaName";
    private const string QueryKey = "Query";
    #endregion

    //protected new readonly string TableName = "Users";
    public override string TableName => "dbo.Users";
    public DataImportSQLServerCreateDataSourceTest() : base(DataSourceType.SQLServer)
    {
        SetupTestDatabase().Wait();
    }
    private const string TestServer = "(localdb)\\MSSQLLocalDB"; // Using LocalDB for tests
    private const string TestDatabase = "MatchLogicTest";
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


    protected override BaseConnectionInfo CreateConnectionInfo()
    {
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        return CreateSqlServerConnectionInfo(parameters);
    }

    private BaseConnectionInfo CreateSqlServerConnectionInfo(Dictionary<string, string> parameters)
    {
        return new BaseConnectionInfo
        {
            Type = DataSourceType.SQLServer,
            Parameters = parameters
        };
    }

    protected override BaseConnectionInfo CreateInvalidConnectionInfo()
    {
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, "Demo" },
            { AuthTypeKey, "Windows" },
        };
        return CreateSqlServerConnectionInfo(parameters);
    }

    #region Create Data Source

    [Fact]
    public async Task CreateDataSource_ValidAuthSQL_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, "sadmin" },
            { PasswordKey, "admin123" },
        };
        var connection = CreateSqlServerConnectionInfo(parameters);
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: connection,
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : TableName,Query : null, ColumnMappings: []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
       
    }
    [Fact]
    public async Task CreateDataSource_WithTableName_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, "sadmin" },
            { PasswordKey, "admin123" },
        };
        var connection = CreateSqlServerConnectionInfo(parameters);
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: connection,
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : TableName,Query : null, ColumnMappings: []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);

    }

    [Fact]
    public async Task CreateDataSource_WithQuery_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, "sadmin" },
            { PasswordKey, "admin123" },
        };
        var connection = CreateSqlServerConnectionInfo(parameters);
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: connection,
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : null,Query : "SELECT * FROM dbo.Users", ColumnMappings: []),
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

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, "sadmin" },
            { PasswordKey, "admin123" },
        };
        var connection = CreateSqlServerConnectionInfo(parameters);
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: connection,
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : TableName,Query : "SELECT * FROM dbo.Users", ColumnMappings: []),
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
        var parameters = new Dictionary<string, string>
        {
            {  "Server", TestServer },
            { "Database", TestDatabase },
            { "AuthType", "Windows" },
        };
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            CreateSqlServerConnectionInfo(parameters),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "[dbo].[Users]",Query : null, ColumnMappings: []),
                                new DataSourceRequest (Name : string.Empty, TableName : "[test].[Orders]",Query : null, ColumnMappings: [])
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
    public async Task CreateDataSource_InvalidDatabase_ShouldReturn_Error(string database)
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var parameters = new Dictionary<string, string>
        {
            {  "Server", TestServer },
            { "Database", database },
            { "AuthType", "Windows" },
        };
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            CreateSqlServerConnectionInfo(parameters),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "[dbo].[Users]",Query : null, ColumnMappings: []),
                                new DataSourceRequest (Name : "DataSource2", TableName : "[test].[Orders]",Query : null, ColumnMappings: [])
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

    [Theory]
    [InlineData(null,null)]
    [InlineData("", "")]
    [InlineData("   ", "    ")]
    public async Task CreateDataSource_SingleDataSource_NullTableNameAndQuery_ShouldReturn_Error(string? tableName,string? query)
    {

        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : tableName,Query : query, ColumnMappings: []),
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
    public async Task CreateDataSource_InvalidServer_ShouldReturn_Error(string server)
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, server },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            CreateSqlServerConnectionInfo(parameters),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1",Query : null, ColumnMappings: []),
                                new DataSourceRequest (Name : "DataSource2", TableName : "Sheet2",Query : null, ColumnMappings: [])
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
    #endregion

    #region Preview Data
    /*[Fact]
    public async Task DataImport_Preview_Data_NullTableName_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            TableName: null, // Null TableName
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("TableName", ValidationMessages.CannotBeNull("TableName")),
        ]);
    }*/

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task DataImport_Preview_Data_NullEmptyTableName_ShouldReturn_Error(string? tableName)
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        var request = new PreviewDataRequest(
            TableName: tableName, // Empty TableName
            Connection: connectionInfo,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "", "Either TableName or Query parameter must be provided.");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Data_InvalidServer_ShouldReturn_Error(string server)
    {

        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, server },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        var connectionInfo = CreateSqlServerConnectionInfo(parameters);
        var request = new PreviewDataRequest(
            TableName : TableName,
            Connection: connectionInfo,
            ColumnMappings: new Dictionary<string, ColumnMapping>()
            {
                { "Name", new ColumnMapping(){
                                SourceColumn = "Name",
                                TargetColumn = "Name1",
                                Include = true
                                }
                },
                { "Email", new ColumnMapping()
                                {
                                SourceColumn = "Email",
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
            { ServerKey, TestServer },
            { DatabaseKey, database },
            { AuthTypeKey, "Windows" },
        };
        var connectionInfo = CreateSqlServerConnectionInfo(parameters);
        var request = new PreviewDataRequest(
            TableName: TableName,
            Connection: connectionInfo,
            ColumnMappings: new Dictionary<string, ColumnMapping>()
            {
                { "Name", new ColumnMapping(){
                                SourceColumn = "Name",
                                TargetColumn = "Name1",
                                Include = true
                                }
                },
                { "Email", new ColumnMapping()
                                {
                                SourceColumn = "Email",
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
    [Fact]
    public async Task DataImport_Preview_Data_WithDuplicateColumnNames_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
            { QueryKey, "SELECT Name,Email FROM [dbo].[Users]" },
        };
        var connectionInfo = CreateSqlServerConnectionInfo(parameters);
        //connectionInfo.FileId = testFile.Id;  // Set the FileId to the test file
        var request = new PreviewDataRequest(
            TableName: TableName,
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            //TableName: TableName, // Default value, override in derived classes if needed
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.SameNameColumnsCount.Should().Be(0);
    }
    #endregion

    #region Preview Columns
    [Fact]
    public async Task DataImport_Preview_Columns_CustomQuery_ShouldReturn_Success()
    {
        // Arrange
        //Project project = await CreateTestProject();
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
            //{ QueryKey, "SELECT Name,Email FROM [dbo].[Users]" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        List<TableInfo> assertColumns = new()
        {
            new TableInfo { Name = "Users", Schema = "dbo", Type = "TABLE" },
            new TableInfo { Name = "Orders", Schema = "test", Type = "TABLE"},
        };
        AssertColumnsSuccessResponse(response,assertColumns);

        response.Value.Metadata.Tables[0].Columns[0].Name.Should().Be("Id");
        response.Value.Metadata.Tables[0].Columns[1].Name.Should().Be("Name");
        response.Value.Metadata.Tables[0].Columns[2].Name.Should().Be("Email");

    }


    [Fact]
    public async Task DataImport_Preview_Columns_WithTable_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        List<TableInfo> assertColumns = new()
        {
            new TableInfo { Name = "Users", Schema = "dbo", Type = "TABLE" },
            new TableInfo { Name = "Orders", Schema = "test", Type = "TABLE"},
        };
        AssertColumnsSuccessResponse(response, assertColumns);

        //response.Value.Metadata.Tables[0].Columns[0].Name.Should().Be("Column0");
        //response.Value.Metadata.Tables[0].Columns[1].Name.Should().Be("Column1");
    }


    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Columns_NullFileId_ShouldReturn_Error(string server)
    {

        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, server },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
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
    public async Task DataImport_Preview_Columns_InvalidFileId_ShouldReturn_Error(string database)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, database },
            { AuthTypeKey, "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
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
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task DataImport_Preview_Tables_NullServer_ShouldReturn_Error(string server)
    {

        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { "Server", server },
            { "Database", TestDatabase },
            { "AuthType", "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
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
            {  "Server", TestServer },
            { "Database", database },
            { "AuthType", "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
        // Act
        var response = await SendRequestPreviewTablesResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
           ValidationMessages.Required($"{DatabaseKey} parameter"),
        ]);
    }

    
    #endregion


    #region Table Name
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
                                new DataSourceRequest (Name : "DataSource1", TableName : String.Empty, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "DataSources[0]", "Either TableName or Query must be provided.");
        //AssertBaseSingleValidationError(response, "DataSources[0].TableName", "Table name is required for each Data Source.");
        //AssertBaseSingleValidationError(response, "DataSources[0].TableName", "Either TableName or Query parameter must be provided.");

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
                                new DataSourceRequest (Name : "DataSource1", TableName : String.Empty, []),
                                new DataSourceRequest (Name : "DataSource2", TableName : String.Empty, [])
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("DataSources[0]", "Either TableName or Query must be provided."),
            //("Connection.Parameters", "Either TableName or Query parameter must be provided."),
            //("DataSources[0].TableName", "Table name is required for each Data Source.")
        ]);
    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_TableName151CharactersLong_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var tableName = new string('A', Application.Common.Constants.FieldLength.NameMaxLength + 1);//151 Characters Long
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name :"DataSource1", TableName : tableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("DataSources[0].TableName", ValidationMessages.MaxLength("Table name",Application.Common.Constants.FieldLength.NameMaxLength)),
            //("DataSources[1].TableName", "Table name is required for each Data Source.")
        ]);
        //AssertBaseSingleValidationError(response, "DataSources[0].TableName", $"Table name must not exceed {Application.Common.Constants.FieldLength.NameMaxLength} characters.");

    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_TableName150CharactersLong_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var tableName = new string('A', Application.Common.Constants.FieldLength.NameMaxLength);//151 Characters Long
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name :"DataSource1", TableName : tableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }
    #endregion

    #region Available Databases
    [Fact]
    public async Task DataImport_AvailableDatabases_ShouldReturn_Success()
    {
        // Arrange
        //Project project = await CreateTestProject();
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);
        // Assert
        AssertBaseSuccessResponse(response);

        response.Value.Count.Should().BeGreaterThan(0);
        //response.Value.Count.Should().Be(5);

    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    //[InlineData("InvalidServer")]
    public async Task DataImport_AvailableDatabases_InvalidServer_ShouldReturn_Error(string server)
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, server },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);
        // Assert
        AssertBaseInvalidResponse(response);

        //response.Value.Count.Should().BeGreaterThan(0);
        //response.Value.Count.Should().Be(5);

    }

    [Fact]
    public async Task DataImport_AvailableDatabases_InaccessibleServer_ShouldReturn_Error()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, "InvalidServer" }, // Invalid Server
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "Windows" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);
        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);
        // Assert
        //AssertBaseInvalidResponse(response);
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.Error);
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().BeNullOrEmpty();
        response.Errors.Should().NotBeEmpty();
        response.Errors.First().Should().Be("A network-related or instance-specific error occurred while establishing a connection to SQL Server. The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server is configured to allow remote connections. (provider: Named Pipes Provider, error: 40 - Could not open a connection to SQL Server)");
        //response.Value.Count.Should().BeGreaterThan(0);
        //response.Value.Count.Should().Be(5);

    }
    #endregion


    #region Test Connection

    [Fact]
    public async Task DataImport_TestConnection_WithInvalidCredentials_ShouldReturn_Error()
    {
        // Arrange
        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestServer },
            { DatabaseKey, TestDatabase },
            { AuthTypeKey, "SQL" },
            { UsernameKey, "sadmin" },
            { PasswordKey, "wrong_password" },
        };
        var request = CreateSqlServerConnectionInfo(parameters);

        // Act
        var response = await SendRequestTestConnectionResponse(request);

        // Assert
        AssertBaseErrorMessage(response, "Connection failed");
        //AssertBaseSuccessResponse(response);
        //response.Value.IsSuccessful.Should().BeFalse();
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
        //response.Value.Metadata.ColumnMappings.Should().NotBeEmpty();
        //response.Value.Metadata.ColumnMappings.Should().HaveCountGreaterThan(1);
        //response.Value.Metadata.ColumnMappings.Should().HaveCount(2);

        for (int i = 0; i < tableInfos.Count; i++)
        {
            response.Value.Metadata.Tables[i].Name.Should().Be(tableInfos[i].Name);
            response.Value.Metadata.Tables[i].Schema.Should().Be(tableInfos[i].Schema);
            response.Value.Metadata.Tables[i].Type.Should().Be(tableInfos[i].Type);
            response.Value.Metadata.Tables[i].Columns.Should().HaveCountGreaterThan(1);
        }       

    }
}
