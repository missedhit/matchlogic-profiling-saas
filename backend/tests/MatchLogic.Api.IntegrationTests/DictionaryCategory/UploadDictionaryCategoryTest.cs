using MatchLogic.Api.Common;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DictionaryCategory.UploadCSV;
using MatchLogic.Api.Handlers.Project.Update;
using MatchLogic.Api.IntegrationTests.Common;
using MatchLogic.Domain.Project;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;
using System.Net.Http.Headers;

namespace MatchLogic.Api.IntegrationTests.DictionaryCategory;
[Collection("DictionaryCategory Tests")]
public class UploadDictionaryCategoryTest : BaseApiTest, IDisposable
{
    private readonly string _csvFilePath, _csvFileWithHeadersPath, _csvFileDuplicatePath;
    private readonly string _fileName = Path.GetRandomFileName() + ".csv";

    public UploadDictionaryCategoryTest() : base(DictionaryCategoryEndpoints.PATH)
    {
        _csvFilePath = Path.Combine(Path.GetTempPath(), _fileName);
        _csvFileWithHeadersPath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + "_withHeaders.csv");
        _csvFileDuplicatePath = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + "_duplicate.csv");
        CreateTestCsvFile();
        CreateTestCsvFileWithHeader();
        CreateTestCsvFileWithHeaderDuplicate();
    }

    #region Positive Test Cases

    [Fact]
    public async Task UploadDictionaryCategory_ValidCSV_ShouldReturn_Success()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "Test Dictionary From CSV",
            Description = "Dictionary created from CSV upload"
        };

        // Act
        var response = await UploadDictionaryCategoryAsync(request);

        // Assert
        AssertSuccessResponse(response, request);
    }


    [Fact]
    public async Task UploadDictionaryCategory_ValidCSVWithHeaders_ShouldReturn_Success()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "Test Dictionary From CSV",
            Description = "Dictionary created from CSV upload"
        };
        var fileName = Path.GetFileName(_csvFileWithHeadersPath);
        var form = new MultipartFormDataContent();
        var fileStream = new FileStream(_csvFileWithHeadersPath, FileMode.Open, FileAccess.Read);
        var streamContent = new StreamContent(fileStream);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
        form.Add(streamContent, "File", fileName);
        // Act
        var response = await UploadDictionaryCategoryAsync(request, form);

        // Assert
        AssertSuccessResponse(response, request);
    }

    #endregion

    #region Negative Test Cases
    /*[Fact]
    public async Task UploadDictionaryCategory_CSVWithDuplicate_ShouldReturn_Exception()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "Test Dictionary From CSV",
            Description = "Dictionary created from CSV upload"
        };
        var fileName = Path.GetFileName(_csvFileDuplicatePath);
        using var form = new MultipartFormDataContent();
        using var fileStream = new FileStream(_csvFileDuplicatePath, FileMode.Open, FileAccess.Read);
        using var streamContent = new StreamContent(fileStream);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
        form.Add(streamContent, "File", fileName);
        // Act
        var exception = await Assert.ThrowsAsync<HttpRequestException>(() => httpClient.PostAndDeserializeAsync<Result<UploadDictionaryCategoryResponse>>(
           $"{RequestURIPath}/File?name={(request.Name ?? "")}&description={(request.Description ?? "")}",
           form));
        // Assert
        exception.Message.Should().Be($"Duplicate item found in category items: 'Item2'");
    }*/
    [Fact]
    public async Task UploadDictionaryCategory_EmptyFile_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "Test Dictionary Empty CSV",
            Description = "Dictionary with empty CSV"
        };

        // Create an empty file
        var fileEmptyPath = Path.GetRandomFileName() + ".csv";
        File.WriteAllText(Path.Combine(Path.GetTempPath(), fileEmptyPath), "");

        // Act
        var response = await UploadDictionaryCategoryAsync(request, fileEmptyPath);

        // Assert
        AssertInvalidResponse(response, "File", ValidationMessages.IsEmpty("File"));
    }

    /*[Fact]
    public async Task UploadDictionaryCategory_NoFile_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "Test Dictionary No File",
            Description = "Dictionary with no file"
        };

        // Act
        var response = await UploadDictionaryCategoryAsync(request, []);

        // Assert
        AssertInvalidResponse(response, "File", "No file was uploaded.");
    }*/

    [Fact]
    public async Task UploadDictionaryCategory_InvalidFileExtension_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "Test Dictionary Invalid Extension",
            Description = "Dictionary with invalid file extension"
        };

        // Create a text file with CSV content
        var fileEmptyPath = Path.GetRandomFileName() + ".txt";
        var txtFilePath = Path.Combine(Path.GetTempPath(), fileEmptyPath);
        File.WriteAllText(txtFilePath, "Item1,Item2,Item3,Item4,Item5");

        // Act
        var response = await UploadDictionaryCategoryAsync(request, fileEmptyPath);

        // Assert
        AssertInvalidResponse(response, "File", ValidationMessages.NotAllowedWithList("File", ".csv"));
    }

    [Fact]
    public async Task UploadDictionaryCategory_InvalidMimeType_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "Test Dictionary Invalid Mime Type",
            Description = "Dictionary with invalid mime type"
        };

        // Act
        var response = await UploadDictionaryCategoryWithInvalidMimeTypeAsync(request);

        // Assert
        AssertInvalidResponse(response, "File", ValidationMessages.Invalid("Mime type"));
    }

    [Fact]
    public async Task UploadDictionaryCategory_MissingName_ShouldReturn_InvalidError()
    {
        // Arrange
        var request = new UploadDictionaryCategoryRequest
        {
            Name = "",
            Description = "Dictionary with missing name"
        };

        // Act
        var response = await UploadDictionaryCategoryAsync(request);

        // Assert
        AssertInvalidResponse(response, "Name", ValidationMessages.Required("Name"));
    }

    #endregion

    #region Helper Methods

    private void CreateTestCsvFile()
    {
        // Create a CSV file with test data
        File.WriteAllText(_csvFilePath, "Item1,Item2,Item3,Item4,Item5");
    }

    private void CreateTestCsvFileWithHeader()
    {
        // Create a CSV file with test data
        File.WriteAllText(_csvFileWithHeadersPath, "Header1,Header2,Header3,Header4,Header5" + Environment.NewLine + "Item1,Item2,Item3,Item4,Item5");
    }

    private void CreateTestCsvFileWithHeaderDuplicate()
    {
        // Create a CSV file with test data
        File.WriteAllText(_csvFileDuplicatePath, "Header1,Header2,Header3,Header4,Header5" + Environment.NewLine + "Item1,Item2,Item2,Item4,Item5");
    }

    private async Task<Result<UploadDictionaryCategoryResponse>> UploadDictionaryCategoryAsync(
        UploadDictionaryCategoryRequest request,
        string customFileName = null)
    {
        var form = new MultipartFormDataContent();

        // Add file if specified
        if (customFileName != null)
        {
            var filePath = Path.Combine(Path.GetTempPath(), customFileName);
            if (File.Exists(filePath))
            {
                var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
                var streamContent = new StreamContent(fileStream);
                streamContent.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
                form.Add(streamContent, "File", customFileName);
            }
        }
        else if (File.Exists(_csvFilePath))
        {
            var fileStream = new FileStream(_csvFilePath, FileMode.Open, FileAccess.Read);
            var streamContent = new StreamContent(fileStream);
            streamContent.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
            form.Add(streamContent, "File", _fileName);
        }

        // Add other form fields
        //form.Add(new StringContent(request.Name ?? ""), "Name");
        //form.Add(new StringContent(request.Description ?? ""), "Description");

        // Send request
        return await UploadDictionaryCategoryAsync(request, form);
    }

    private async Task<Result<UploadDictionaryCategoryResponse>> UploadDictionaryCategoryWithInvalidMimeTypeAsync(
        UploadDictionaryCategoryRequest request)
    {
        var form = new MultipartFormDataContent();

        // Add file with incorrect mime type
        var fileStream = new FileStream(_csvFilePath, FileMode.Open, FileAccess.Read);
        var streamContent = new StreamContent(fileStream);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/pdf"); // Invalid mime type for CSV
        form.Add(streamContent, "File", _fileName);

        // Add other form fields
        //form.Add(new StringContent(request.Name ?? ""), "Name");
        //form.Add(new StringContent(request.Description ?? ""), "Description");

        // Send request
        return await UploadDictionaryCategoryAsync(request, form);
    }
    private async Task<Result<UploadDictionaryCategoryResponse>> UploadDictionaryCategoryAsync(
        UploadDictionaryCategoryRequest request,
        MultipartFormDataContent form)
    {
        return await httpClient.PostAndDeserializeAsync<Result<UploadDictionaryCategoryResponse>>(
           $"{RequestURIPath}/File?name={(request.Name ?? "")}&description={(request.Description ?? "")}",
           form);
    }
    private void AssertSuccessResponse(Result<UploadDictionaryCategoryResponse> response, UploadDictionaryCategoryRequest request)
    {
        AssertBaseSuccessResponse(response);
        response.Value.Should().NotBeNull();
        response.Value.Id.Should().NotBe(Guid.Empty);
        response.Value.Name.Should().Be(request.Name);
        response.Value.Description.Should().Be(request.Description);
        response.Value.Items.Should().NotBeNull();
        response.Value.Items.Should().HaveCountGreaterThan(0);
        response.Value.Version.Should().BeGreaterThan(0);
    }

    private void AssertInvalidResponse(Result<UploadDictionaryCategoryResponse> response, string identifier, string errorMessage)
    {
        AssertBaseSingleValidationError(response, identifier, errorMessage);
    }

    /*public void Dispose()
    {
        // Clean up test files
        if (File.Exists(_csvFilePath))
        {
            File.Delete(_csvFilePath);
        }

        var emptyFilePath = Path.Combine(Path.GetTempPath(), "empty.csv");
        if (File.Exists(emptyFilePath))
        {
            File.Delete(emptyFilePath);
        }

        var invalidFilePath = Path.Combine(Path.GetTempPath(), "invalid.txt");
        if (File.Exists(invalidFilePath))
        {
            File.Delete(invalidFilePath);
        }
    }*/

    #endregion
}
