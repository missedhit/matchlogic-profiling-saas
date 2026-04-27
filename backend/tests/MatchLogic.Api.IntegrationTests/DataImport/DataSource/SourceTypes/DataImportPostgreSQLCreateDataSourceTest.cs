using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Preview.Columns;
using MatchLogic.Api.Handlers.DataSource.Preview.Data;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
using MatchLogic.Domain.Import;
using Npgsql;
using ColumnMapping = MatchLogic.Domain.Project.ColumnMapping;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource.SourceTypes;

[Collection("Data Import PostgreSQL")]
public class DataImportPostgreSQLCreateDataSourceTest : DataSourceTypeTest<BaseConnectionInfo>, IDisposable
{
    #region  Keys
    private const string ServerKey = "Server";
    private const string PortKey = "Port";
    private const string DatabaseKey = "Database";
    private const string UsernameKey = "Username";
    private const string PasswordKey = "Password";
    private const string QueryKey = "Query";
    #endregion

    public override string TableName => "public.Users";
    
    // Test environment settings
    private const string TestHost = "localhost";
    private const string TestPort = "5432";
    private readonly string TestDatabase = "dataladd_test";
    private const string TestUsername = "postgres";
    private const string TestPassword = "admin123";
    
    public DataImportPostgreSQLCreateDataSourceTest() : base(DataSourceType.PostgreSQL)
    {
        TestDatabase = $"MatchLogictest_{Guid.NewGuid():N}";
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
    

    public void Dispose()
    {
        // Cleanup: Drop test database and role
        using var connection = new NpgsqlConnection(
            $"Host={TestHost};Port={TestPort};Database=postgres;Username={TestUsername};Password={TestPassword}");
        connection.Open();

        // Disconnect all active sessions first
        var killSessionsCmd = $@"
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE datname = '{TestDatabase}' AND pid <> pg_backend_pid()";
                
        using (var command = new NpgsqlCommand(killSessionsCmd, connection))
        {
            command.ExecuteNonQuery();
        }

        // Drop the database
        var dropDbCmd = $"DROP DATABASE IF EXISTS {TestDatabase}";
        using (var command = new NpgsqlCommand(dropDbCmd, connection))
        {
            command.ExecuteNonQuery();
        }

        // Drop the test user
        var dropUserCmd = "DROP ROLE IF EXISTS testuser";
        using (var command = new NpgsqlCommand(dropUserCmd, connection))
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
        return CreatePostgreSqlConnectionInfo(parameters);
    }

    private BaseConnectionInfo CreatePostgreSqlConnectionInfo(Dictionary<string, string> parameters)
    {
        return new BaseConnectionInfo
        {
            Type = DataSourceType.PostgreSQL,
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
        return CreatePostgreSqlConnectionInfo(parameters);
    }

    #region Create Data Source

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
        
        var connection = CreatePostgreSqlConnectionInfo(parameters);
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
    
    [Fact]
    public async Task CreateDataSource_TestUser_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var parameters = new Dictionary<string, string>
        {
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, "testuser" },
            { PasswordKey, "testpass" }
        };
        
        var connection = CreatePostgreSqlConnectionInfo(parameters);
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

    [Fact]
    public async Task CreateDataSource_WithTableName_ShouldReturn_Success()
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
                new DataSourceRequest(Name: "DataSource1", TableName: TableName, Query: null, ColumnMappings: []),
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

        var connection = CreateConnectionInfo();
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo: connection,
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: null, Query: "SELECT * FROM public.users", ColumnMappings: []),
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
                new DataSourceRequest(Name: "DataSource1", TableName: TableName, Query: "SELECT * FROM public.users", ColumnMappings: []),
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
                new DataSourceRequest(Name: "DataSource1", TableName: "public.users", Query: null, ColumnMappings: []),
                new DataSourceRequest(Name: string.Empty, TableName: "test.orders", Query: null, ColumnMappings: [])
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
            { ServerKey, TestHost },
            { PortKey, TestPort },
            { DatabaseKey, database },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };
        
        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            CreatePostgreSqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: "public.users", Query: null, ColumnMappings: []),
                new DataSourceRequest(Name: "DataSource2", TableName: "test.orders", Query: null, ColumnMappings: [])
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
            CreatePostgreSqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: "public.users", Query: null, ColumnMappings: []),
                new DataSourceRequest(Name: "DataSource2", TableName: "test.orders", Query: null, ColumnMappings: [])
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
            CreatePostgreSqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: "public.users", Query: null, ColumnMappings: []),
                new DataSourceRequest(Name: "DataSource2", TableName: "test.orders", Query: null, ColumnMappings: [])
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
            CreatePostgreSqlConnectionInfo(parameters),
            dataSources:
            [
                new DataSourceRequest(Name: "DataSource1", TableName: "public.users", Query: null, ColumnMappings: []),
                new DataSourceRequest(Name: "DataSource2", TableName: "test.orders", Query: null, ColumnMappings: [])
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

    #region Preview Data

    //[Fact]
    //public async Task DataImport_Preview_Data_WithMapping_WithValidConnection_ShouldReturn_Success1()
    //{
    //    // Arrange
    //    var connectionInfo = CreateConnectionInfo();
    //    //connectionInfo.Parameters["TableName"] = TableName;
    //    var request = new PreviewDataRequest(
    //        TableName: TableName, // Default value, override in derived classes if needed
    //        connectionInfo,
    //        //Connection: ConvertIntoJsonObject(connectionInfo),
    //        //TableName: TableName, // Default value, override in derived classes if needed
    //        ColumnMappings: new Dictionary<string, ColumnMapping>()
    //        {
    //            { "Name", new ColumnMapping(){
    //                            SourceColumn = "Name",
    //                            TargetColumn = "Name1",
    //                            Include = true
    //                            }
    //            },
    //            //{ "Age", new ColumnMapping()
    //            //                {
    //            //                SourceColumn = "Age",
    //            //                TargetColumn = "Age1",
    //            //                Include = true
    //            //                }
    //            //}
    //        }
    //    );

    //    // Act
    //    var response = await SendRequestPreviewDataResponse(request);

    //    // Assert
    //    AssertDataSuccessResponse(response);
    //}
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
        
        var connectionInfo = CreatePostgreSqlConnectionInfo(parameters);
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
        
        var connectionInfo = CreatePostgreSqlConnectionInfo(parameters);
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
            { QueryKey, "SELECT name, email FROM public.users" },
        };
        
        var connectionInfo = CreatePostgreSqlConnectionInfo(parameters);
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
        
        var connectionInfo = CreatePostgreSqlConnectionInfo(parameters);
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        
        // Assert
        List<TableInfo> assertColumns = new()
        {
            new TableInfo { Name = "users", Schema = "public", Type = "TABLE" },
            new TableInfo { Name = "orders", Schema = "test", Type = "TABLE"},
        };
        
        AssertColumnsSuccessResponse(response, assertColumns);

        response.Value.Metadata.Tables[0].Columns[0].Name.ToLower().Should().Be("id");
        response.Value.Metadata.Tables[0].Columns[1].Name.ToLower().Should().Be("name");
        response.Value.Metadata.Tables[0].Columns[2].Name.ToLower().Should().Be("email");
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
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

    #region Available Databases
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);
        
        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Count.Should().BeGreaterThan(0);
        response.Value.Should().Contain(TestDatabase);
        response.Value.Should().Contain("postgres");
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
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
            { ServerKey, "invalid-host" }, // Invalid Host
            { PortKey, TestPort },
            { DatabaseKey, TestDatabase },
            { UsernameKey, TestUsername },
            { PasswordKey, TestPassword }
        };
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);
        
        // Assert
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.Error);
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().BeNullOrEmpty();
        response.Errors.Should().NotBeEmpty();
    }
    
    [Fact]
    public async Task DataImport_AvailableDatabases_InvalidCredentials_ShouldReturn_Error()
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
        // Act
        var response = await SendRequestAvailableDatabasesResponse(request);
        
        // Assert
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.Error);
        response.IsSuccess.Should().BeFalse();
    }
    #endregion
    
    #region Test Connection
    
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
        
        var request = CreatePostgreSqlConnectionInfo(parameters);
        
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

        for (int i = 0; i < tableInfos.Count; i++)
        {
            response.Value.Metadata.Tables[i].Name.ToLower().Should().Be(tableInfos[i].Name);
            response.Value.Metadata.Tables[i].Schema.Should().Be(tableInfos[i].Schema);
            response.Value.Metadata.Tables[i].Type.Should().Be(tableInfos[i].Type);
            response.Value.Metadata.Tables[i].Columns.Should().HaveCountGreaterThan(1);
        }
    }
}