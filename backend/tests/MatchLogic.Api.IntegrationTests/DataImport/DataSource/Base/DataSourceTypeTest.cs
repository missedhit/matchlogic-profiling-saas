using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Preview.Columns;
using MatchLogic.Api.Handlers.DataSource.Preview.Data;
using MatchLogic.Api.Handlers.DataSource.Preview.Tables;
using MatchLogic.Api.Handlers.DataSource.TestConnection;
using MatchLogic.Domain.Import;
using System.Text.Json.Nodes;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
public abstract class DataSourceTypeTest<TConnectionInfo> : BaseDataSourceTest
    where TConnectionInfo : BaseConnectionInfo
{
    protected readonly string SourceTypeStr;
    protected readonly DataSourceType SourceTypeEnum;
    public virtual string TableName => "Sheet1"; // Default value, override in derived classes if needed

    protected DataSourceTypeTest(DataSourceType sourceType) : base()
    {
        SourceTypeEnum = sourceType;
        SourceTypeStr = sourceType.ToString();
    }

    // Force implementing classes to configure their specific connection info
    protected abstract TConnectionInfo CreateConnectionInfo();
    protected abstract TConnectionInfo CreateInvalidConnectionInfo();


    protected async Task<Result<PreviewTablesResponse>> SendRequestPreviewTablesResponse( object request)
    {
        return await SendRequest<PreviewTablesResponse>("/Preview/Tables", SourceTypeStr, request);
    }

    protected async Task<Result<PreviewColumnsResponse>> SendRequestPreviewColumnsResponse(object request)
    {
        return await SendRequest<PreviewColumnsResponse>("/Preview/Columns", SourceTypeStr, request);
    }

    protected async Task<Result<PreviewDataResponse>> SendRequestPreviewDataResponse(object request)
    {
        return await SendRequest<PreviewDataResponse>("/Preview/Data", SourceTypeStr, request);
    }

    protected async Task<Result<TestConnectionResponse>> SendRequestTestConnectionResponse(object request)
    {
        return await SendRequest<TestConnectionResponse>("/Preview/TestConnection", SourceTypeStr, request);
    }

    protected async Task<Result<List<string>>>SendRequestAvailableDatabasesResponse(object request)
    {
        return await SendRequest<List<string>>("/Preview/Databases", SourceTypeStr, request);
    }
    protected async Task<Result<CreateDataSourceResponse>> SendRequestCreateDataSourceResponse(object request)
    {
        return await SendRequest<CreateDataSourceResponse>("/DataSource", SourceTypeStr, request);
    }

    #region Create Data Source
    // Common test for creating data source
    [Fact]
    public async Task CreateDataSource_SingleDataSource_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
        //response.Value.DataSources.Should().NotBeEmpty();
    }
    
    [Fact]
    public async Task CreateDataSource_SingleDataSource_NullName_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : null, TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        if (request.Connection.Type is DataSourceType.SQLServer or DataSourceType.PostgreSQL or DataSourceType.MySQL)
            AssertBaseValidationErrors(response, [
                ("DataSources[0].Name", "'Name' must not be empty."),
                ("DataSources[0].Name", "Name is required for each Data Source.")
                ]);
        else
            AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_EmptyName_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : string.Empty, TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        if(request.Connection.Type is DataSourceType.SQLServer or DataSourceType.PostgreSQL or DataSourceType.MySQL)
            AssertBaseSingleValidationError(response, "DataSources[0].Name", "Name is required for each Data Source.");
        else
            AssertBaseSuccessResponse(response);
        //response.Value.DataSources.Should().NotBeEmpty();
    }

    

    [Fact]
    public async Task CreateDataSource_MultipleDataSource_ValidRequest_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1", []),
                                new DataSourceRequest (Name : "DataSource2", TableName : "Sheet2", [])
                            ]
                        );
        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertCreateDataSourceResponseSuccess(response, request);
    }


    [Fact]
    public async Task CreateDataSource_SingleDataSource_NUllConnection_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: null, // NULL Connection
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection", "'Connection' must not be empty."),
            //("ConnectionInfo", "Connection cannot be null"),
        ]);
    }

    /*[Fact]
    public async Task CreateDataSource_SingleDataSource_EmptyConnection_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();        
        var request =  new CreateDataSourceRequest
        (
            ProjectId: testProject.Id,
            Connection: new JsonObject(),
            DataSources: [ new DataSourceRequest (Name : "DataSource1", TableName : TableName, []),]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection", "Connection cannot be null");
    }*/

    [Fact]
    public async Task CreateDataSource_NullProjectId_ShouldReturn_Error()
    {
        // Arrange
        var request = CreateDataSourceRequest
                        (
                            projectId: Guid.Empty,//Empty Project ID
                            CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1", []),
                                new DataSourceRequest (Name : "DataSource2", TableName : "Sheet2", [])
                            ]
                        );
        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            //("ProjectId", "'Project Id' must not be empty."),
            ("ProjectId", ValidationMessages.Required("Project ID")),
            ("ProjectId", ValidationMessages.NotExists("Project")),
        ]);
    }
    [Fact]
    public async Task CreateDataSource_InvalidProjectId_ShouldReturn_Error()
    {
        // Arrange
        var request = CreateDataSourceRequest
                        (
                            projectId: Guid.NewGuid(), // Assuming this ID does not exist
                            CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1", []),
                                new DataSourceRequest (Name : "DataSource2", TableName : "Sheet2", [])
                            ]
                        );
        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        // Assert
        AssertBaseSingleValidationError(response, "ProjectId", ValidationMessages.NotExists("Project"));
    }
    
    [Fact]
    public async Task CreateDataSource_EmptyDataSources_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            CreateConnectionInfo(),
                            dataSources: [] //Empty Data Source
                        );
        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        // Assert
        AssertBaseSingleValidationError(response, "DataSources", "At least one Data Source is required. Data Source list should not be empty.");
    }

    [Fact]
    public async Task CreateDataSource_DuplicateDataSourceNames_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            CreateConnectionInfo(),
                            dataSources: // Duplicate Data Source Name
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1", []),
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet2", [])
                            ]
                        );
        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "DataSources", ValidationMessages.MustBeUniqueInList("DataSource name"));

    }

    [Fact]
    public async Task CreateDataSource_NonUniqueDataSourceNameInProject_ShouldReturn_Error()
    {
        // Arrange
        var existingDataSource = await AddTestSingleDataSource();// Create an existing data source Name: DataSource1
        var request = CreateDataSourceRequest
                        (
                            projectId: existingDataSource.ProjectId, //Set Same ProjectId
                            CreateConnectionInfo(),
                            dataSources: // Duplicate Data Source Name as existing one : DataSource1
                            [
                                new DataSourceRequest (Name : existingDataSource.Name,
                                TableName : existingDataSource.Configuration.TableOrSheet,
                                []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        // Assert
        AssertBaseSingleValidationError(response, "DataSources", ValidationMessages.AlreadyExists("DataSource name"));
    }


    [Fact]
    public async Task CreateDataSource_WithNullParameters_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = null; //Null Parameters
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: connectionInfo,
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeNull("Parameters"));
    }

    [Fact]
    public async Task CreateDataSource_WithEmptyParameters_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: connectionInfo,
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeEmpty("Parameters"));
    }


    
    [Fact]
    public async Task CreateDataSource_SingleDataSource_Name151CharactersLong_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var name = new string('A', Application.Common.Constants.FieldLength.NameMaxLength + 1);//151 Characters Long
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : name, TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        //AssertBaseSingleValidationError(response, "DataSources[0].Name", $"Name must not exceed {Application.Common.Constants.FieldLength.NameMaxLength} characters.");
        AssertBaseSingleValidationError(response, "DataSources[0].Name", ValidationMessages.MaxLength("Name", Application.Common.Constants.FieldLength.NameMaxLength));

    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_Name150CharactersLong_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();

        var name = new string('A', Application.Common.Constants.FieldLength.NameMaxLength);//151 Characters Long
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : name, TableName : TableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertCreateDataSourceResponseSuccess(response, request);
        //AssertBaseSingleValidationError(response, "DataSources[0].TableName", $"Table name must not exceed {Application.Common.Constants.FieldLength.NameMaxLength} characters.");

    }


    private void AssertCreateDataSourceResponseSuccess(Result<CreateDataSourceResponse> response, CreateDataSourceRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.ProjectRun.Should().NotBeNull();
        response.Value.ProjectRun.Id.Should().NotBe(Guid.Empty);
        response.Value.ProjectRun.ProjectId.Should().NotBe(Guid.Empty);
        response.Value.ProjectRun.ProjectId.Should().Be(request.ProjectId);
        response.Value.ProjectRun.PreviousRunId.Should().Be(Guid.Empty);
        //response.Value.ProjectRun.StartTime.Date.Should().Be(DateTime.Now.Date);
        response.Value.ProjectRun.EndTime.Should().BeNull();
        response.Value.ProjectRun.DataImportResult.Should().BeNull();
        response.Value.ProjectRun.RunNumber.Should().Be(0);
        response.Value.ProjectRun.Status.Should().Be(RunStatus.InProgress);
    }

    #endregion 
    
    #region Preview Tables
    // Common test for preview tables
    [Fact]
    public async Task DataImport_Preview_Tables_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();

        // Act
        var response = await SendRequestPreviewTablesResponse(connectionInfo);

        // Assert
        AssertTableSuccessResponse(response);
    }

    [Fact]
    public async Task DataImport_Preview_Tables_WithNullParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = null; //Null Parameters

        // Act
        var response = await SendRequestPreviewTablesResponse(connectionInfo);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.Parameters", ValidationMessages.CannotBeNull("Parameters")),
            //("ConnectionInfo.Parameters", "Parameters cannot be empty")
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Tables_WithEmptyParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters
   
        // Act
        var response = await SendRequestPreviewTablesResponse(connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeEmpty("Parameters"));
    }
    /*[Fact]
    public async Task DataImport_Preview_Tables_WithInvalidSourceType_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters
   
        // Act
        var response = await SendRequest<PreviewTablesResponse>("/Preview/Tables", "InvalidSourceType", connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "SourceType", ValidationMessages.Invalid("SourceType"));
    }*/

    private void AssertTableSuccessResponse(Result<PreviewTablesResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Tables.Should().NotBeEmpty();

        response.Value.Tables[0].Name.Should().NotBeNullOrEmpty();
        //response.Value.Tables[0].Schema.Should().NotBeNullOrEmpty();


        /*response.Value.Tables.Should().HaveCountGreaterThan(1);
        response.Value.Tables.Should().HaveCount(2);

        response.Value.Tables[0].Name.Should().Be("Sheet1");
        response.Value.Tables[0].Schema.Should().Be("Excel");
        response.Value.Tables[0].Name.Should().Be("Sheet1");
        response.Value.Tables[1].Schema.Should().Be("Excel");*/
    }

    #endregion

    #region Preview Columns
    // Common test for preview columns
    [Fact]
    public async Task DataImport_Preview_Columns_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        var requestObj = new JsonObject();

        // Convert connection info to JsonObject
        requestObj = ConvertIntoJsonObject(connectionInfo);

        // Add table name 
        requestObj["TableName"] = JsonValue.Create(TableName); // Default value, override in derived classes if needed

        // Act
        var response = await SendRequestPreviewColumnsResponse(requestObj);

        // Assert
        AssertBaseSuccessResponse(response);
    }
    /*[Fact]
    public async Task DataImport_Preview_Columns_WithInvalidSourceType_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters

        // Act
        var response = await SendRequest<PreviewTablesResponse>("/Preview/Columns", "InvalidSourceType", connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "SourceType", ValidationMessages.Invalid("SourceType"));
    }*/
    /*[Fact]
    public async Task DataImport_Preview_Columns_WithNullParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = null; //Null Parameters

        // Act
        var response = await SendRequestPreviewColumnsResponse(connectionInfo);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("ConnectionInfo.Parameters", "Parameters cannot be null"),
            //("ConnectionInfo.Parameters", "Parameters cannot be empty")
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Columns_WithEmptyParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters

        // Act
        var response = await SendRequestPreviewColumnsResponse(connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "Parameters cannot be empty");
    }*/

    


    #endregion

    #region Preview Data

    // Common test for preview data
    [Fact]
    public async Task DataImport_Preview_Data_WithEmptyMapping_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        //connectionInfo.Parameters["TableName"] = TableName; // Set TableName in Parameters if required by the data source
        var request = new PreviewDataRequest(
            Connection : connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            TableName: TableName, // Default value, override in derived classes if needed
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }
    [Fact]
    public async Task DataImport_Preview_Data_WithMapping_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        //connectionInfo.Parameters["TableName"] = TableName;
        var request = new PreviewDataRequest(
            TableName: TableName, // Default value, override in derived classes if needed
            connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            //TableName: TableName, // Default value, override in derived classes if needed
            ColumnMappings: new Dictionary<string, ColumnMapping>()
            {
                { "Name", new ColumnMapping(){
                                SourceColumn = "Name",
                                TargetColumn = "Name1",
                                Include = true
                                }
                },
                //{ "Age", new ColumnMapping()
                //                {
                //                SourceColumn = "Age",
                //                TargetColumn = "Age1",
                //                Include = true
                //                }
                //}
            }
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertDataSuccessResponse(response);
    }


    /*[Fact]
    public async Task DataImport_Preview_Data_WithInvalidSourceType_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters

        // Act
        var response = await SendRequest<PreviewTablesResponse>("/Preview/Data", "InvalidSourceType", connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "SourceType", ValidationMessages.Invalid("SourceType"));
    }*/

    [Fact]
    public async Task DataImport_Preview_Data_WithNullParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = null; //Null Parameters

        var request = new PreviewDataRequest(
            TableName: TableName, // Default value, override in derived classes if needed
            Connection : connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            //TableName: TableName, // Default value, override in derived classes if needed
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeNull("Parameters"));
    }

    [Fact]
    public async Task DataImport_Preview_Data_WithEmptyParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters
        var request = new PreviewDataRequest(
            TableName: TableName, // Default value, override in derived classes if needed
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            //TableName: TableName, // Default value, override in derived classes if needed
            ColumnMappings: []
        );
        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeEmpty("Parameters"));
    }
    //[Fact]
    //public async Task DataImport_Preview_Data_NullTableName_ShouldReturn_Error()
    //{
    //    // Arrange
    //    var connectionInfo = CreateConnectionInfo();
    //    //connectionInfo.Parameters["TableName"] = null;
    //    var request = new PreviewDataRequest(
    //        Connection: connectionInfo,
    //        TableName: null, // Null TableName
    //        ColumnMappings: []
    //    );

    //    // Act
    //    var response = await SendRequestPreviewDataResponse(request);


    //    // Assert
    //    //AssertBaseSuccessResponse(response);
    //    AssertBaseValidationErrors(response,
    //    [
    //        ("TableName", ValidationMessages.CannotBeNull("TableName")),
    //        //("TableName", "'Table Name' must not be empty."),
    //        //("TableName", "Table name is required.")
    //    ]);
    //}

    //[Fact]
    //public async Task DataImport_Preview_Data_EmptyTableName_ShouldReturn_Error()
    //{
    //    // Arrange
    //    var connectionInfo = CreateConnectionInfo();
    //    //connectionInfo.Parameters["TableName"] = string.Empty;
    //    var request = new PreviewDataRequest(
    //        TableName: string.Empty, // Empty TableName
    //        Connection: connectionInfo,            
    //        ColumnMappings: []
    //    );

    //    // Act
    //    var response = await SendRequestPreviewDataResponse(request);

    //    // Assert
    //    //AssertBaseSuccessResponse(response);
    //    //AssertBaseSingleValidationError(response, "TableName", "Table name is required.");
    //    AssertBaseSingleValidationError(response, "TableName", ValidationMessages.CannotBeEmpty("TableName"));
    //}
    private void AssertDataSuccessResponse(Result<PreviewDataResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Data.Should().NotBeEmpty();

        var litst = response.Value.Data[0].Keys.ToList();

        response.Value.Data[0].Keys.Contains("Name1");
        response.Value.Data[0].Keys.Contains("Age1");
    }


    #endregion


    #region Test Connection

    [Fact]
    public async Task DataImport_TestConnection_WithNullParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = null; //Null Parameters

        //var request = new TestConnectionRequest(connectionInfo);

        // Act
        var response = await SendRequestTestConnectionResponse(connectionInfo);

        // Assert
        //AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeEmpty("Parameters"));
        AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeNull("Parameters"));
    }

    [Fact]
    public async Task DataImport_TestConnection_WithValidParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();

        //var request = new TestConnectionRequest(connectionInfo);

        // Act
        var response = await SendRequestTestConnectionResponse(connectionInfo);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.IsSuccessful.Should().BeTrue();
    }

    #endregion




    #region Parameters
    [Fact]
    public async Task DataImport_Preview_Columns_WithNullParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = null; //Null Parameters

        // Act
        var response = await SendRequestPreviewColumnsResponse(connectionInfo);

        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.Parameters", ValidationMessages.CannotBeNull("Parameters")),
            //("ConnectionInfo.Parameters", "Parameters cannot be empty")
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Columns_WithEmptyParameters_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters = []; //Empty Parameters

        // Act
        var response = await SendRequestPreviewColumnsResponse(connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeEmpty("Parameters"));
    }
    #endregion
}

