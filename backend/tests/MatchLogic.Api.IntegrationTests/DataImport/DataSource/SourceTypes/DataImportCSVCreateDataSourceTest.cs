using MatchLogic.Api.Common;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.Handlers.DataSource.Preview.Columns;
using MatchLogic.Api.Handlers.DataSource.Preview.Data;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;
using MatchLogic.Domain.Import;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource.SourceTypes;
public class DataImportCSVCreateDataSourceTest : DataSourceTypeTest<BaseConnectionInfo>
{
    public DataImportCSVCreateDataSourceTest() : base(DataSourceType.CSV)
    {
    }


    private async Task<FileImport> CreateUploadFile(Guid projectId)
    {
        var filePath = Path.Combine(Path.GetTempPath(), "testCSV1.csv");
       
        System.IO.File.WriteAllText(filePath, "Name,Age\nJohn Doe,30\nJane Smith,25");

        return await new FileImportBuilder(GetServiceProvider())
            .WithProjectId(projectId)
            .WithFilePath(filePath)
            .WithDataSourceType(DataSourceType.CSV)
            .BuildAsync();
    }

    protected override BaseConnectionInfo CreateConnectionInfo()
    {

        var projectId = CreateTestProject().GetAwaiter().GetResult().Id;
        var fileImport = CreateUploadFile(projectId).GetAwaiter().GetResult();

        return CreateCSVConnectionInfo(fileImport.Id, true);
    }

    private BaseConnectionInfo CreateCSVConnectionInfo(Guid FileId, bool? hasHeaders,string? TableName = null)
    {

        var parameters = new Dictionary<string, string>
        {
            { "FileId", FileId.ToString() },
            { "HasHeaders", hasHeaders.ToString()! }
        };

        if (!string.IsNullOrEmpty(TableName))
        {
            parameters.Add("TableName", TableName);
        }

        return new BaseConnectionInfo
        {
            Type = DataSourceType.CSV,
            Parameters = parameters
        };        
    }

    protected override BaseConnectionInfo CreateInvalidConnectionInfo()
    {
        return CreateCSVConnectionInfo(Guid.Empty, true);
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
                            CreateCSVConnectionInfo(fileId, true),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
                            ]
                        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", "'FileId' must not be empty."),
            ("Connection.FileId", ValidationMessages.Required("File Id")),
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")

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
                            CreateCSVConnectionInfo(fileId, true),
                            dataSources:
                            [
                                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
                            ]
                        );
        // Act
        var response = await SendRequestCreateDataSourceResponse(request);
        //var response = await CreateDataSourceAsync(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")
        ]);
    }

    [Fact]
    public async Task CreateDataSource_SingleDataSource_NullTableName_ShouldReturn_Success()
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
        AssertBaseSuccessResponse(response);
        //response.Value.DataSources.Should().NotBeEmpty();
    }
    #endregion

    #region Preview Data

//    [Fact]
//    public async Task DataImport_Preview_Data_NullTableName_ShouldReturn_Error()
//    {
//        // Arrange
//        var connectionInfo = CreateConnectionInfo();
//        connectionInfo.Parameters["TableName"] = null;
//        var request = new PreviewDataRequest(
//            Connection: connectionInfo,
//            //Connection: ConvertIntoJsonObject(connectionInfo),
//            //TableName: null, // Null TableName
//            ColumnMappings: []
//        );
        
//        // Act
//        var response = await SendRequestPreviewDataResponse(request);


//        // Assert
//        AssertBaseSuccessResponse(response);
///*        AssertBaseValidationErrors(response,
//        [
//            ("TableName", "'Table Name' must not be empty."),
//            ("TableName", "Table name is required.")
//        ]);*/
//    }

//    [Fact]
//    public async Task DataImport_Preview_Data_EmptyTableName_ShouldReturn_Error()
//    {
//        // Arrange
//        var connectionInfo = CreateConnectionInfo();
//        connectionInfo.Parameters["TableName"] = string.Empty;
//        var request = new PreviewDataRequest(
//            Connection: connectionInfo,
//            //Connection: ConvertIntoJsonObject(connectionInfo),
//            //TableName: string.Empty, // Null TableName
//            ColumnMappings: []
//        );

//        // Act
//        var response = await SendRequestPreviewDataResponse(request);

//        // Assert
//        AssertBaseSuccessResponse(response);
//        //AssertBaseSingleValidationError(response, "TableName", "Table name is required.");
//    }
    [Fact]
    public async Task DataImport_Preview_Data_NullFileId_ShouldReturn_Error()
    {

        // Arrange
        var invalidFileId = Guid.Empty;
        var connectionInfo = CreateCSVConnectionInfo(invalidFileId, true ,"");
        var request = new PreviewDataRequest(
            Connection : connectionInfo,
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
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", "'FileId' must not be empty."),
            ("Connection.FileId", ValidationMessages.Required("File Id")),
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Data_InvalidFileId_ShouldReturn_Error()
    {
        // Arrange
        var invalidFileId = Guid.NewGuid(); // Assuming this ID does not exist
        var connectionInfo = CreateCSVConnectionInfo(invalidFileId, true, TableName);
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
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")
        ]);
    }
    [Fact]
    public async Task DataImport_Preview_Data_WithDuplicateColumnNames_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();

        var filePath = Path.Combine(Path.GetTempPath(), "testCSV1.csv");
        System.IO.File.WriteAllText(filePath, "Name,Age,Name,Age\nJohn Doe,30,Brett Pierce,40\nJane Smith,25,Byran Dumphries,28");
        FileImport testFile = await new FileImportBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .WithFilePath(filePath)
            .WithDataSourceType(DataSourceType.CSV)
            .BuildAsync();

        var connectionInfo = CreateCSVConnectionInfo(testFile.Id, true ,TableName);
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
    [Fact]
    public async Task DataImport_Preview_Data_HeaderFalse_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();

        var filePath = Path.Combine(Path.GetTempPath(), "testCSV1WithoutHeader.csv");
        System.IO.File.WriteAllText(filePath, "item1,item2,item3,item4");
        FileImport testFile = await new FileImportBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .WithFilePath(filePath)
            .WithDataSourceType(DataSourceType.CSV)
            .BuildAsync();

        var connectionInfo = CreateCSVConnectionInfo(testFile.Id, false,TableName);
        //connectionInfo.FileId = testFile.Id;  // Set the FileId to the test file
        var request = new PreviewDataRequest(
            TableName: TableName,
            Connection: connectionInfo,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
        response.Value.Data.Count.Should().Be(1);
        response.Value.TotalRecords.Should().Be(1L);
        //response.Value.SameNameColumnsCount.Should().Be(1L);
    }

    [Fact]
    public async Task DataImport_Preview_Data_HeaderTrue_WithValidConnection_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();

        var filePath = Path.Combine(Path.GetTempPath(), "testCSV1WithoutHeader.csv");
        System.IO.File.WriteAllText(filePath, "item1,item2,item3,item4");
        FileImport testFile = await new FileImportBuilder(GetServiceProvider())
            .WithProjectId(project.Id)
            .WithFilePath(filePath)
            .WithDataSourceType(DataSourceType.CSV)
            .BuildAsync();

        var connectionInfo = CreateCSVConnectionInfo(testFile.Id, true, TableName);
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
        response.Value.Data.Count.Should().Be(0);
        response.Value.TotalRecords.Should().Be(0);
        response.Value.SameNameColumnsCount.Should().Be(0);
    }
    #endregion

    #region Preview Columns
    [Fact]
    public async Task DataImport_Preview_Columns_HasHeaderTrue_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();
        FileImport testFile = await CreateUploadFile(project.Id);
        var request = CreateCSVConnectionInfo(testFile.Id, true);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertCSVSuccessResponse(response);

        response.Value.Metadata.Tables[0].Columns[0].Name.Should().Be("Name");
        response.Value.Metadata.Tables[0].Columns[1].Name.Should().Be("Age");

    }

    protected void AssertCSVSuccessResponse(Result<PreviewColumnsResponse> response)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Metadata.Should().NotBeNull();


        response.Value.Metadata.Tables.Should().NotBeEmpty();
        //response.Value.Metadata.Tables.Should().HaveCountGreaterThan(1);
        response.Value.Metadata.Tables.Should().HaveCount(1);

        response.Value.Metadata.ColumnMappings.Should().BeEmpty();
        //response.Value.Metadata.ColumnMappings.Should().NotBeEmpty();
        //response.Value.Metadata.ColumnMappings.Should().HaveCountGreaterThan(1);
        //response.Value.Metadata.ColumnMappings.Should().HaveCount(2);

        response.Value.Metadata.Tables[0].Name.Should().NotBeNullOrWhiteSpace();
        response.Value.Metadata.Tables[0].Schema.Should().BeNull();

        //response.Value.Metadata.Tables[1].Name.Should().Be("Sheet2");
        //response.Value.Metadata.Tables[1].Schema.Should().Be("Excel");


        response.Value.Metadata.Tables[0].Columns.Should().HaveCount(2);
        //response.Value.Metadata.Tables[1].Columns.Should().HaveCount(0);

    }


    [Fact]
    public async Task DataImport_Preview_Columns_HasHeaderFalse_ShouldReturn_Success()
    {
        // Arrange
        Project project = await CreateTestProject();
        FileImport testFile = await CreateUploadFile(project.Id);
        var request = CreateCSVConnectionInfo(testFile.Id, false);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertCSVSuccessResponse(response);

        //response.Value.Metadata.Tables[0].Columns[0].Name.Should().Be("Column0");
        //response.Value.Metadata.Tables[0].Columns[1].Name.Should().Be("Column1");
    }


    [Fact]
    public async Task DataImport_Preview_Columns_NullFileId_ShouldReturn_Error()
    {

        // Arrange
        var invalidFileId = Guid.Empty;
        var request = CreateCSVConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", "'FileId' must not be empty."),
            ("Connection.FileId", ValidationMessages.Required("File Id")),
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Columns_InvalidFileId_ShouldReturn_Error()
    {
        // Arrange
        var invalidFileId = Guid.NewGuid(); // Assuming this ID does not exist
        var request = CreateCSVConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewColumnsResponse(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")
        ]);
    }
    #endregion

    #region Preview Tables
    [Fact]
    public async Task DataImport_Preview_Tables_NullFileId_ShouldReturn_Error()
    {

        // Arrange
        var invalidFileId = Guid.Empty;
        var request = CreateCSVConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewTablesResponse(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", "'FileId' must not be empty."),
            ("Connection.FileId", ValidationMessages.Required("File Id")),
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")
        ]);
    }

    [Fact]
    public async Task DataImport_Preview_Tables_InvalidFileId_ShouldReturn_Error()
    {
        // Arrange
        var invalidFileId = Guid.NewGuid(); // Assuming this ID does not exist
        var request = CreateCSVConnectionInfo(invalidFileId, false);
        // Act
        var response = await SendRequestPreviewTablesResponse(request);
        // Assert
        AssertBaseValidationErrors(response,
        [
            ("Connection.FileId", ValidationMessages.NotExists("File")),
            ("Connection.FileId", ValidationMessages.NotExists("Directory path")),
            ("Connection.Parameters", "File is not a valid CSV file.")
        ]);
    }
    #endregion


    #region Paramaters Validation for CSV
    

    #region Quote Parameter Validation Tests

    [Fact]
    public async Task CreateDataSource_ValidQuoteParameter_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "\""; // Valid quote character

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task CreateDataSource_QuoteParameterNullValue_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = null; // Null quote value

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute character '' cannot be Empty or null.");
        //AssertBaseSingleValidationError(response, "Connection.Parameters[1]", ValidationMessages.CannotBeNull("Parameters"));
    }

    [Fact]
    public async Task CreateDataSource_QuoteParameterEmptyValue_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = ""; // Empty quote value

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute character '' cannot be Empty or null.");
    }

    [Fact]
    public async Task CreateDataSource_QuoteParameterWhitespaceValue_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = " "; // Whitespace quote value

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute character ' ' cannot be a WhiteSpaceChar.");
    }

    [Fact]
    public async Task CreateDataSource_QuoteParameterMultipleCharacters_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "ab"; // Multiple characters

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute must be a single character.");
    }

    /*[Fact]
    public async Task CreateDataSource_QuoteParameterCarriageReturn_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "\r"; // Carriage return

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "The Qoute '\r' cannot be a line ending. (\r,\n,\r\n)");
    }

    [Fact]
    public async Task CreateDataSource_QuoteParameterLineFeed_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "\n"; // Line feed

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "The Qoute '\n' cannot be a line ending. (\r,\n,\r\n)");
    }

    [Fact]
    public async Task CreateDataSource_QuoteParameterCarriageReturnLineFeed_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "\r\n"; // Carriage return + line feed

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "The Qoute must be a single character.");
    }*/

    #endregion

    #region Delimiter Parameter Validation Tests

    [Fact]
    public async Task CreateDataSource_ValidDelimiterParameter_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = ","; // Valid delimiter character

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task CreateDataSource_DelimiterParameterNullValue_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = null; // Null delimiter value

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Delimiter character '' cannot be Empty or null.");
        //AssertBaseSingleValidationError(response, "Connection.Parameters", ValidationMessages.CannotBeNull("Parameters"));
    }

    [Fact]
    public async Task CreateDataSource_DelimiterParameterEmptyValue_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = ""; // Empty delimiter value

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Delimiter character '' cannot be Empty or null.");
    }

    [Fact]
    public async Task CreateDataSource_DelimiterParameterWhitespaceValue_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = " "; // Whitespace delimiter value

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Delimiter character ' ' cannot be a WhiteSpaceChar.");
    }

    [Fact]
    public async Task CreateDataSource_DelimiterParameterMultipleCharacters_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "ab"; // Multiple characters

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Delimiter must be a single character.");
    }

    /*[Fact]
    public async Task CreateDataSource_DelimiterParameterCarriageReturn_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "\r"; // Carriage return

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "The Delimiter '\r' cannot be a line ending. (\r,\n,\r\n)");
    }

    [Fact]
    public async Task CreateDataSource_DelimiterParameterLineFeed_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "\n"; // Line feed

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "The Delimiter '\n' cannot be a line ending. (\r,\n,\r\n)");
    }

    [Fact]
    public async Task CreateDataSource_DelimiterParameterCarriageReturnLineFeed_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "\r\n"; // Carriage return + line feed

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "The Delimiter must be a single character.");
    }*/

    #endregion

    #region Quote and Delimiter Difference Validation Tests

    [Fact]
    public async Task CreateDataSource_SameQuoteAndDelimiterParameters_ShouldReturn_Error()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = ",";
        connectionInfo.Parameters["Delimiter"] = ","; // Same as quote

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute character and the Delimiter cannot be the same.");
    }

    [Fact]
    public async Task CreateDataSource_DifferentQuoteAndDelimiterParameters_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "\"";
        connectionInfo.Parameters["Delimiter"] = ","; // Different from quote

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    #endregion

    #region Valid Quote Characters Tests

    [Fact]
    public async Task CreateDataSource_QuoteParameterSingleQuote_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "'"; // Single quote

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task CreateDataSource_QuoteParameterPipe_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "|"; // Pipe character

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    #endregion

    #region Valid Delimiter Characters Tests

    [Fact]
    public async Task CreateDataSource_DelimiterParameterSemicolon_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = ";"; // Semicolon

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    /*[Fact]
    public async Task CreateDataSource_DelimiterParameterTab_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "\t"; // Tab character

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }*/

    [Fact]
    public async Task CreateDataSource_DelimiterParameterPipe_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "|"; // Pipe character

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    #endregion

    #region Missing Parameters Tests

    [Fact]
    public async Task CreateDataSource_MissingQuoteParameter_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        // Don't add Quote parameter - should be valid (optional)

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    [Fact]
    public async Task CreateDataSource_MissingDelimiterParameter_ShouldReturn_Success()
    {
        // Arrange
        Project testProject = await CreateTestProject();
        var connectionInfo = CreateConnectionInfo();
        // Don't add Delimiter parameter - should be valid (optional)

        var request = CreateDataSourceRequest
        (
            projectId: testProject.Id,
            connectionInfo,
            dataSources:
            [
                new DataSourceRequest (Name : "DataSource1", TableName : "TableName", []),
            ]
        );

        // Act
        var response = await SendRequestCreateDataSourceResponse(request);

        // Assert
        AssertBaseSuccessResponse(response);
    }

    #endregion

    #region Preview Data Parameters Validation Tests

    [Fact]
    public async Task DataImport_Preview_Data_InvalidQuoteParameter_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "\\n"; // Invalid quote (line ending)
        connectionInfo.Parameters["TableName"] = TableName;

        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            TableName: TableName,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        //AssertBaseSingleValidationError(response, "ConnectionInfo.Parameters", "The Qoute '\\n' cannot be a line ending. (\\r,\\n,\\r\\n)");
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute must be a single character.");
    }

    [Fact]
    public async Task DataImport_Preview_Data_InvalidDelimiterParameter_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = ""; // Invalid delimiter (empty)
        connectionInfo.Parameters["TableName"] = TableName;
        var request = new PreviewDataRequest(
            Connection: connectionInfo,
            //Connection: ConvertIntoJsonObject(connectionInfo),
            TableName: TableName,
            ColumnMappings: []
        );

        // Act
        var response = await SendRequestPreviewDataResponse(request);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Delimiter character '' cannot be Empty or null.");
    }

    #endregion

    #region Preview Columns Parameters Validation Tests

    [Fact]
    public async Task DataImport_Preview_Columns_InvalidQuoteParameter_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = "ab"; // Invalid quote (multiple characters)

        // Act
        var response = await SendRequestPreviewColumnsResponse(connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute must be a single character.");
    }

    [Fact]
    public async Task DataImport_Preview_Columns_InvalidDelimiterParameter_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "\r"; // Invalid delimiter (line ending)

        // Act
        var response = await SendRequestPreviewColumnsResponse(connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Delimiter '\r' cannot be a line ending. (\r,\n,\r\n)");
    }

    #endregion

    #region Preview Tables Parameters Validation Tests

    [Fact]
    public async Task DataImport_Preview_Tables_InvalidQuoteParameter_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Qoute"] = " "; // Invalid quote (whitespace)

        // Act
        var response = await SendRequestPreviewTablesResponse(connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Qoute character ' ' cannot be a WhiteSpaceChar.");
    }

    [Fact]
    public async Task DataImport_Preview_Tables_InvalidDelimiterParameter_ShouldReturn_Error()
    {
        // Arrange
        var connectionInfo = CreateConnectionInfo();
        connectionInfo.Parameters["Delimiter"] = "xy"; // Invalid delimiter (multiple characters)

        // Act
        var response = await SendRequestPreviewTablesResponse(connectionInfo);

        // Assert
        AssertBaseSingleValidationError(response, "Connection.Parameters", "The Delimiter must be a single character.");
    }

    #endregion

    #endregion

}
