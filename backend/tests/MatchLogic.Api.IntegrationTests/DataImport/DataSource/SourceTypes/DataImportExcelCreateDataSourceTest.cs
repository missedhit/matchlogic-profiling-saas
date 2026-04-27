using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Preview.Columns;
using MatchLogic.Api.Handlers.DataSource.Preview.Data;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
using MatchLogic.Domain.Import;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource.SourceTypes;
[Collection("Data Import Excel")]
public class DataImportExcelCreateDataSourceTest : DataSourceTypeTest<BaseConnectionInfo>
{
    public DataImportExcelCreateDataSourceTest() : base(DataSourceType.Excel)
    {        
    }

    private async Task<FileImport> CreateUploadFile(Guid projectId)
    {
        return await new FileImportBuilder(GetServiceProvider())
            .WithProjectId(projectId)
            .CreateTestExcelFile()
            .BuildAsync();        
    }

    protected override BaseConnectionInfo CreateConnectionInfo()
    {

        var projectId = CreateTestProject().GetAwaiter().GetResult().Id;
        var fileImport = CreateUploadFile(projectId).GetAwaiter().GetResult();

        return CreateExcelConnectionInfo(fileImport.Id, true);
    }

    private BaseConnectionInfo CreateExcelConnectionInfo(Guid FileId, bool? hasHeaders,string? TableName = null)
    {
        var parameters = new Dictionary<string, string>
        {
            { "FileId", FileId.ToString() },
            { "HasHeaders", hasHeaders.ToString()! }
        };

        if(!string.IsNullOrEmpty(TableName))
        {
            parameters.Add("TableName", TableName);
        }

        return new BaseConnectionInfo
        {
            Type = DataSourceType.Excel,
            Parameters = parameters
        };
    }

    protected override BaseConnectionInfo CreateInvalidConnectionInfo()
    {
        return CreateExcelConnectionInfo(Guid.Empty, true);
    }

    #region Create Data Source

    [Fact]
    public async Task CreateDataSource_NullFileId_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        Guid fileId = Guid.Empty; // Empty File ID
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            CreateExcelConnectionInfo(fileId, true),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1", []),
                                new DataSourceRequest (Name : "DataSource2", TableName : "Sheet2", [])
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "'FileId' must not be empty.",
            ValidationMessages.Required("File Id"),
            ValidationMessages.NotExists("File"),
            ValidationMessages.NotExists("Directory path"),
            "File is not a valid Excel file.",
        ]);
    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_NullTableName_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            connectionInfo: CreateConnectionInfo(),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : null, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "'Table Name' must not be empty.",
            "Table name is required for each Data Source."
        ]);
    }
    [Fact]
    public async Task CreateDataSource_InvalidFileId_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        Guid fileId = Guid.NewGuid(); // Assuming this ID does not exist
        var request = CreateDataSourceRequest
                        (
                            projectId: testProject.Id,
                            CreateExcelConnectionInfo(fileId, true),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "Sheet1", []),
                                new DataSourceRequest (Name : "DataSource2", TableName : "Sheet2", [])
                            ]
                        );
        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "File does not exist.",
            "Directory path does not exist."
        ]);
    }
    #endregion

    #region Preview Data

    [Fact]
    public async Task DataImport_Preview_Data_NullTableName_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        //connectionInfo.Parameters["TableName"] = null;
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            TableName: null, // Null TableName
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);


        // Assert
        //AssertBaseSuccessResponse(response);
        AssertBaseValidationErrors(response,
        [
            ("TableName", ValidationMessages.CannotBeNull("TableName")),
            //("TableName", "'Table Name' must not be empty."),
            //("TableName", "Table name is required.")
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Data_EmptyTableName_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        //connectionInfo.Parameters["TableName"] = string.Empty;
        var request = new PreviewDataRequest(
            TableName: string.Empty, // Empty TableName
            Connection: connectionInfo,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        //AssertBaseSuccessResponse(response);
        //AssertBaseSingleValidationError(response, "TableName", "Table name is required.");
        AssertBaseSingleValidationError(response, "TableName", ValidationMessages.CannotBeEmpty("TableName"));
    }
   /* [Fact]
    public async Task DataImport_Preview_Data_NullTableName_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["TableName"] = null;
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            TableName: null, // Null TableName
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);


        // Assert
        AssertBaseSuccessResponse(response);
        *//*        AssertBaseValidationErrors(response,
                [
                    ("TableName", "'Table Name' must not be empty."),
                    ("TableName", "Table name is required.")
                ]);*//*
    }

    [Fact]
    public async Task DataImport_Preview_Data_EmptyTableName_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["TableName"] = string.Empty;
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            //TableName: string.Empty, // Null TableName
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
        //AssertBaseSingleValidationError(response, "TableName", "Table name is required.");
    }*/
    [Fact]
    public async Task DataImport_Preview_Data_NullFileId_ShouldReturn_Error()
    {

        // Arrange
        var invalidFileId = Guid.Empty;
        var connectionInfo = CreateExcelConnectionInfo(invalidFileId, true, "Sheet1");
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            TableName: "Sheet1",
            ColumnMappings: new Dictionary<string, ColumnMapping>()
            {
                { "Name", new ColumnMapping(){
                                SourceColumn = "Name",
                                TargetColumn = "Name1",
                                Include = true
                                }
                },
                { "Age", new ColumnMapping()
                                {
                                SourceColumn = "Age",
                                TargetColumn = "Age1",
                                Include = true
                                }
                }
            });
        // Act
        var response = await SendRequestPreviewDataResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
             "'FileId' must not be empty.",
            ValidationMessages.Required("File Id"),
            ValidationMessages.NotExists("File"),
            ValidationMessages.NotExists("Directory path"),
            "File is not a valid Excel file.",

        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Data_InvalidFileId_ShouldReturn_Error()
    {
        // Arrange
        var invalidFileId = Guid.NewGuid(); // Assuming this ID does not exist
        var connectionInfo = CreateExcelConnectionInfo(invalidFileId, true, TableName);
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            TableName: TableName,
            ColumnMappings: new Dictionary<string, ColumnMapping>()
            {
                { "Name", new ColumnMapping(){
                                SourceColumn = "Name",
                                TargetColumn = "Name1",
                                Include = true
                                }
                },
                { "Age", new ColumnMapping()
                                {
                                SourceColumn = "Age",
                                TargetColumn = "Age1",
                                Include = true
                                }
                }
            });
        // Act
        var response = await SendRequestPreviewDataResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "File does not exist.",
            "Directory path does not exist."
        ]);
    }
    [Fact]
    public async Task DataImport_Preview_Data_WithDuplicateColumnNames_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();
        FileImport testFile = await new FileImportBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .CreateDuplicateTestFile()
            .BuildAsync();
        var connectionInfo = CreateExcelConnectionInfo(testFile.Id, true,TableName); 
        //connectionInfo.FileId = testFile.Id;  // Set the FileId to the test file
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            TableName: TableName, // Default value, override in derived classes if needed
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.SameNameColumnsCount.Should().Be(2);
    }
    #endregion

    #region Preview Columns
    [Fact]
    public async Task DataImport_Preview_Columns_HasHeaderTrue_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();
        FileImport testFile = await CreateUploadFile(project.Id);
        var request = CreateExcelConnectionInfo(testFile.Id, true);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertColumnsSuccessResponse(response);

        response.Value.Metadata.Tables[0].Columns[0].Name.Should().Be("Name");
        response.Value.Metadata.Tables[0].Columns[1].Name.Should().Be("Age");

    }


    [Fact]
    public async Task DataImport_Preview_Columns_HasHeaderFalse_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();
        FileImport testFile = await CreateUploadFile(project.Id);
        var request = CreateExcelConnectionInfo(testFile.Id, false);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertColumnsSuccessResponse(response);

        //response.Value.Metadata.Tables[0].Columns[0].Name.Should().Be("Column0");
        //response.Value.Metadata.Tables[0].Columns[1].Name.Should().Be("Column1");
    }


    [Fact]
    public async Task DataImport_Preview_Columns_NullFileId_ShouldReturn_Error()
    {

        // Arrange
        var invalidFileId = Guid.Empty;
        var request = CreateExcelConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
             "'FileId' must not be empty.",
            ValidationMessages.Required("File Id"),
            ValidationMessages.NotExists("File"),
            ValidationMessages.NotExists("Directory path"),
            "File is not a valid Excel file.",
           
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Columns_InvalidFileId_ShouldReturn_Error()
    {
        // Arrange
        var invalidFileId = Guid.NewGuid(); // Assuming this ID does not exist
        var request = CreateExcelConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "File does not exist.",
            "Directory path does not exist."
        ]);
    }
    #endregion

    #region Preview Tables
    [Fact]
    public async Task DataImport_Preview_Tables_NullFileId_ShouldReturn_Error()
    {

        // Arrange
        var invalidFileId = Guid.Empty;
        var request = CreateExcelConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewTablesResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
             "'FileId' must not be empty.",
            ValidationMessages.Required("File Id"),
            ValidationMessages.NotExists("File"),
            ValidationMessages.NotExists("Directory path"),
            "File is not a valid Excel file.",           
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Tables_InvalidFileId_ShouldReturn_Error()
    {
        // Arrange
        var invalidFileId = Guid.NewGuid(); // Assuming this ID does not exist
        var request = CreateExcelConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewTablesResponse(request);
        // Assert
        AssertBaseValidationErrorsContainMessages(response,
        [
            "File does not exist.",
            "Directory path does not exist."
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
        AssertBaseSingleValidationError(response, "DataSources[0].TableName", "Table name is required for each Data Source.");

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
            ("DataSources[0].TableName", "Table name is required for each Data Source."),
            //("DataSources[1].TableName", "Table name is required for each Data Source.")
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
                                new DataSourceRequest (Name : string.Empty, TableName : tableName, []),
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
                                new DataSourceRequest (Name : string.Empty, TableName : tableName, []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }
    #endregion


    protected void AssertColumnsSuccessResponse(Result<PreviewColumnsResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Metadata.Should().NotBeNull();


        response.Value.Metadata.Tables.Should().NotBeEmpty();
        response.Value.Metadata.Tables.Should().HaveCountGreaterThan(1);
        response.Value.Metadata.Tables.Should().HaveCount(2);

        response.Value.Metadata.ColumnMappings.Should().BeEmpty();
        //response.Value.Metadata.ColumnMappings.Should().NotBeEmpty();
        //response.Value.Metadata.ColumnMappings.Should().HaveCountGreaterThan(1);
        //response.Value.Metadata.ColumnMappings.Should().HaveCount(2);

        response.Value.Metadata.Tables[0].Name.Should().Be("Sheet1");
        response.Value.Metadata.Tables[0].Schema.Should().Be("Excel");

        response.Value.Metadata.Tables[1].Name.Should().Be("Sheet2");
        response.Value.Metadata.Tables[1].Schema.Should().Be("Excel");


        response.Value.Metadata.Tables[0].Columns.Should().HaveCount(2);
        response.Value.Metadata.Tables[1].Columns.Should().HaveCount(0);

    }

}
