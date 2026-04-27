using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Application.Interfaces.Project;
using MatchLogic.Domain.Import;
using NPOI.XSSF.UserModel;
using System.Net.Http.Headers;
using FileUploadRequest = MatchLogic.Api.Handlers.File.Upload.FileUploadRequest;
using FileUploadResponse = MatchLogic.Api.Handlers.File.Upload.FileUploadResponse;

namespace MatchLogic.Api.IntegrationTests.DataImport.File;
[Collection("Data Import Upload File Tests")]
public class DataImportFileUploadTest : BaseApiTest, IDisposable
{
    private readonly IProjectService projectService;
    private readonly string _testExcelPath;
    private readonly string _fileName = Path.GetRandomFileName() + ".xlsx";

    public DataImportFileUploadTest() : base($"{DataImportEndpoints.PATH}/file")
    {
        projectService = GetService<IProjectService>();
        _testExcelPath = Path.Combine(Path.GetTempPath(), _fileName);
        CreateTestExcelFile();
    }


    [Fact]
    public async Task UploadFile_Excel_ValidFile_ShouldReturn_Success()
    {
        // Arrange
        FileUploadRequest request = await CreateFileRequestAsync();
        // Act
        var response = await UploadFileAsync(request);
        // Assert
        AssertSuccessResponse(response, request);
    }
    [Fact]
    public async Task UploadFile_InvalidSourceType_ShouldReturn_Error()
    {
        // Arrange
        // Create a test project
        Project testProject = await new ProjectBuilder(GetServiceProvider())
            .BuildAsync();

        // Create File Upload Request       
        FileUploadRequest request = new FileUploadRequest()
        {
            ProjectId = testProject.Id,
            SourceType = "AnyDataSource" // Invalid Data Source
        };
        // Act
        var response = await UploadFileAsync(request);
        // Assert
        AssertInvalidResponse(response, "SourceType", ValidationMessages.Invalid("SourceType"));
    }
    /*
        [Fact]
        public async Task UploadFile_NullFile_ShouldReturn_Error()
        {
            // Arrange
            FileUploadRequest request = CreateFileRequest();

            using var form = new MultipartFormDataContent();
            // Do not add any file to the form to simulate a null file scenario

            // Act
            var response = await UploadFileAsync(request, form);
            // Assert
            AssertTableInvalidResponse(response, "File", "File is required.");
        }*/

    [Fact]
    public async Task UploadFile_EmptyFile_ShouldReturn_Error()
    {
        // Arrange

        var form = new MultipartFormDataContent();
        var streamContent = new StreamContent(Stream.Null);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        form.Add(streamContent, "file", "emptyfile.xlsx");
        var request = await CreateFileRequestAsync();

        // Act
        var response = await UploadFileAsync(request, form);
        // Assert
        AssertInvalidResponse(response, "File", ValidationMessages.IsEmpty("File"));
    }

    [Fact]
    public async Task UploadFile_InvalidExtension_ShouldReturn_Error()
    {
        // Arrange
        var form = new MultipartFormDataContent();
        var streamContent = new StreamContent(new MemoryStream(new byte[1]));
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        form.Add(streamContent, "file", "testfile.txt");
        var request = await CreateFileRequestAsync();
        // Act
        var response = await UploadFileAsync(request, form);
        // Assert
        AssertInvalidResponse(response, "File", $"File extension is not allowed. Allowed extensions are: {string.Join(", ", ApiConstants.AllowedExtensions)}");
    }

    [Fact]
    public async Task UploadFile_InvalidMimeType_ShouldReturn_Error()
    {
        // Arrange
        var form = new MultipartFormDataContent();

        var fileStream = new FileStream(_testExcelPath, FileMode.Open, FileAccess.Read);
        var streamContent = new StreamContent(fileStream);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/pdf");
        form.Add(streamContent, "file", Path.GetFileName(_testExcelPath));

        var request = await CreateFileRequestAsync();
        // Act
        var response = await UploadFileAsync(request, form);
        // Assert
        AssertInvalidResponse(response, "File", ValidationMessages.Invalid("Mime type"));

    }

    [Fact]
    public async Task UploadFile_InvalidProjectId_ShouldReturn_Error()
    {
        // Arrange
        var request = await CreateFileRequestAsync();
        request.ProjectId = Guid.NewGuid(); // Invalid Project ID
        // Act
        var response = await UploadFileAsync(request);
        // Assert
        AssertInvalidResponse(response, "ProjectId", ValidationMessages.NotExists("Project"));
    }

    private async Task<FileUploadRequest> CreateFileRequestAsync()
    {
        // Create a test project
        Project testProject = await new ProjectBuilder(GetServiceProvider())
            .BuildAsync();

        // Create File Upload Request       
        var request = new FileUploadRequest()
        {
            ProjectId = testProject.Id,
            SourceType = "Excel"
        };
        return request;
    }
    private async Task<Result<FileUploadResponse>> UploadFileAsync(FileUploadRequest request)
    {
        var form = new MultipartFormDataContent();
        var fileStream = new FileStream(_testExcelPath, FileMode.Open, FileAccess.Read);
        var streamContent = new StreamContent(fileStream);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        form.Add(streamContent, "file", Path.GetFileName(_testExcelPath));

        try
        {
            return await UploadFileAsync(request, form);
        }
        finally
        {
            streamContent.Dispose();
            fileStream.Dispose();
            form.Dispose();
        }
    }

    private async Task<Result<FileUploadResponse>> UploadFileAsync(FileUploadRequest request, MultipartFormDataContent form)
    {
        var uri = $"{RequestURIPath}?projectId={request.ProjectId}&sourceType={request.SourceType}";
        return await httpClient.PostAndDeserializeAsync<Result<FileUploadResponse>>(uri, form);
    }


    private void AssertSuccessResponse(Result<FileUploadResponse> response, FileUploadRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Id.Should().NotBe(Guid.Empty);
        response.Value.ProjectId.Should().NotBe(Guid.Empty);
        response.Value.ProjectId.Should().Be(request.ProjectId);
        response.Value.FileName.Should().NotBeNullOrEmpty();
        response.Value.OriginalName.Should().NotBeNullOrEmpty();
        response.Value.OriginalName.Should().Be(_fileName);
        response.Value.FilePath.Should().NotBeNullOrEmpty();
        response.Value.FileSize.Should().BeGreaterThan(0);
        response.Value.FileExtension.Should().NotBeNullOrEmpty();
        response.Value.DataSourceType.Should().Be(DataSourceType.Excel.ToString());
        response.Value.CreatedDate.Date.Should().Be(DateTime.Now.Date);
    }
    private void AssertInvalidResponse(Result<FileUploadResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    private void CreateTestExcelFile()
    {
        using (var workbook = new XSSFWorkbook())
        {
            var sheet = workbook.CreateSheet("Sheet1");
            var sheet2 = workbook.CreateSheet("Sheet2");

            // Create header row
            var headerRow = sheet.CreateRow(0);
            headerRow.CreateCell(0).SetCellValue("Name");
            headerRow.CreateCell(1).SetCellValue("Age");

            // Add test data
            var row1 = sheet.CreateRow(1);
            row1.CreateCell(0).SetCellValue("John Doe");
            row1.CreateCell(1).SetCellValue(30);

            var row2 = sheet.CreateRow(2);
            row2.CreateCell(0).SetCellValue("Jane Smith");
            row2.CreateCell(1).SetCellValue(25);

            using (var fileStream = new FileStream(_testExcelPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                workbook.Write(fileStream);
            }
        }
    }

    /*public void Dispose()
    {
        if (System.IO.File.Exists(TestTempExcelPath))
        {
            System.IO.File.Delete(TestTempExcelPath);
        }
        base.Dispose();
    }*/
}
