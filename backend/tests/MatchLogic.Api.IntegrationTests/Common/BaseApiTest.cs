using Azure;
using MatchLogic.Domain.Import;
using Microsoft.Extensions.DependencyInjection;

namespace MatchLogic.Api.IntegrationTests.Common;
public abstract class BaseApiTest : IClassFixture<WebAPIFactory>, IDisposable
{
    protected string RequestURIPath;

    public HttpClient httpClient { get; private set; } = null!;
    internal WebAPIFactory factory { get; private set; } = default!;

    public IServiceScope Scope { get; set; } = default!;

    //internal readonly string UploadFolderPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "MatchLogicApi", "Uploads");
    //internal readonly string UploadTempFolder = Path.Combine(Path.GetTempPath() + "MatchLogicIntegrationTestApi");

    protected BaseApiTest(string uri)
    {
        if (string.IsNullOrWhiteSpace(uri))
        {
            throw new ArgumentException($"'{nameof(uri)}' cannot be null or whitespace.", nameof(uri));
        }

        this.RequestURIPath = string.Format("api/{0}", uri);
        this.Initialize();
    }

    public void Dispose()
    {
        factory.Dispose();
        httpClient.Dispose();
        Scope.Dispose();
    }

    public IServiceProvider GetServiceProvider()
    {
        return Scope.ServiceProvider;
    }
    public T GetService<T>() where T : notnull
    {
        return GetServiceProvider().GetRequiredService<T>();
    }
    public void Initialize()
    {
        factory = new WebAPIFactory();
        httpClient = factory.CreateClient();
        Scope = factory.Services.CreateScope();
        //Directory.CreateDirectory(UploadFolderPath);
        //Directory.CreateDirectory(UploadTempFolder);

    }

    protected string GetExtensionFromSource(DataSourceType type) => type switch
    {
        DataSourceType.Excel => ".xlsx",
        DataSourceType.CSV => ".csv",
        _ => throw new NotSupportedException($"Data source type {type} is not supported."),
    };

    #region Assertions Methods
    /// <summary>
    /// Asserts that the response has a status of 'NotFound'.
    /// Validates that the response is not null, the status is 'NotFound',
    /// IsSuccess is false, ValidationErrors is not empty, and Errors is not empty.
    /// </summary>
    /// <typeparam name="TResponse">The type of the response.</typeparam>
    /// <param name="response">The response to validate.</param>
    protected void AssertBaseNotFoundResponseError<TResponse>(Result<TResponse> response, string message)
    {
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.NotFound);
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().BeEmpty();
        response.Errors.Should().NotBeNullOrEmpty();
        response.Errors.First().Should().Be(message);
    }
    /// <summary>
    /// Asserts that the response is successful with a status of 'Ok'.
    /// </summary>
    /// <typeparam name="TResponse">The type of the response.</typeparam>
    /// <param name="response">The response to validate.</param>
    protected void AssertBaseSuccessResponse<TResponse>(Result<TResponse> response)
    {
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.Ok);
        response.IsSuccess.Should().BeTrue();
        response.ValidationErrors.Should().BeNullOrEmpty();
        response.Errors.Should().BeNullOrEmpty();
    }

    protected void AssertBaseSuccessResponse<TResponse>(Result<TResponse> response, ResultStatus status)
    {
        response.Should().NotBeNull();
        response.Status.Should().Be(status);
        response.IsSuccess.Should().BeTrue();
        response.ValidationErrors.Should().BeNullOrEmpty();
        response.Errors.Should().BeNullOrEmpty();
    }
    /// <summary>
    /// Asserts that the response is invalid with a status of 'Invalid'.
    /// </summary>
    protected void AssertBaseInvalidResponse<TResponse>(Result<TResponse> response)
    {
        AssertBaseInvalidResponseWithStatus(ResultStatus.Invalid, response);
    }



    /// <summary>
    /// Asserts that the response has a status of 'NotFound'.
    /// Validates that the response is not null, the status is 'NotFound',
    /// IsSuccess is false, ValidationErrors is not empty, and Errors is not empty.
    /// </summary>
    /// <typeparam name="TResponse">The type of the response.</typeparam>
    /// <param name="response">The response to validate.</param>
    protected void AssertBaseNotFoundResponseError<TResponse>(Result<TResponse> response)
    {
        response.Should().NotBeNull();
        response.Status.Should().Be(ResultStatus.NotFound);
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().NotBeNullOrEmpty();
        //response.Errors.Should().NotBeNullOrEmpty();
    }
    /// <summary>
    /// Asserts that the response is invalid with the specified status.
    /// Checks that the response is not null, status matches, IsSuccess is false,
    /// ValidationErrors is not empty, and Errors is empty.
    /// </summary>
    /// <typeparam name="TResponse">The type of the response.</typeparam>
    /// <param name="status">The expected status of the response.</param>
    /// <param name="response">The response to validate.</param>
    protected void AssertBaseInvalidResponseWithStatus<TResponse>(ResultStatus status, Result<TResponse> response)
    {
        response.Should().NotBeNull();
        response.Status.Should().Be(status);
        response.IsSuccess.Should().BeFalse();
        response.ValidationErrors.Should().NotBeNullOrEmpty();
        response.Errors.Should().BeNullOrEmpty();
    }

    /// <summary>
    /// Asserts that the response contains a single validation error with the specified identifier and error message.
    /// </summary>
    /// <typeparam name="TResponse">The type of the response.</typeparam>
    /// <param name="response">The response to validate.</param>
    /// <param name="identifier">The expected identifier of the validation error.</param>
    /// <param name="errorMessage">The expected error message of the validation error.</param>
    protected void AssertBaseSingleValidationError<TResponse>(Result<TResponse> response, string identifier, string errorMessage)
    {
        AssertBaseInvalidResponse(response);
        response.ValidationErrors.Count().Should().Be(1);
        response.ValidationErrors.First().Identifier.Should().Be(identifier);
        response.ValidationErrors.First().ErrorMessage.Should().Be(errorMessage);
        response.Value.Should().BeNull();
    }
    /// <summary>
    /// Asserts that the response contains the expected validation errors.
    /// </summary>
    /// <typeparam name="TResponse">The type of the response.</typeparam>
    /// <param name="response">The response to validate.</param>
    /// <param name="errors">The expected validation errors List.</param>
    protected void AssertBaseValidationErrors<TResponse>(Result<TResponse> response, List<(string Identifier, string ErrorMessage)> errors)
    {
        AssertBaseInvalidResponse(response);
        response.ValidationErrors.Count().Should().Be(errors.Count);
        var validationErrors = response.ValidationErrors.ToList();
        for (int i = 0; i < errors.Count; i++)
        {
            validationErrors[i].Identifier.Should().Be(errors[i].Identifier);
            validationErrors[i].ErrorMessage.Should().Be(errors[i].ErrorMessage);
        }
        response.Value.Should().BeNull();
    }


    /// <summary>
    /// Asserts that the response contains the specified validation error messages.
    /// Validates that the response is invalid, the validation error messages contain all specified errors,
    /// and the response value is null.
    /// </summary>
    /// <typeparam name="TResponse">The type of the response.</typeparam>
    /// <param name="response">The response to validate.</param>
    /// <param name="errors">An array of expected validation error messages.</param>
    protected void AssertBaseValidationErrorsContainMessages<TResponse>(Result<TResponse> response, string[] errors)
    {
        AssertBaseInvalidResponse(response);
        var validationErrorMessages = response.ValidationErrors
            .Select(e => e.ErrorMessage)
            .ToList();

        foreach (var error in errors)
        {
            validationErrorMessages.Should().Contain(error);
        }

        response.Value.Should().BeNull();
    }

    protected void AssertBaseExceptionMessage(Result response, string message)
    {
        response.Should().NotBeNull();
        response.IsSuccess.Should().BeFalse();
        response.Status.Should().Be(ResultStatus.Error);
        response.Errors.Should().NotBeEmpty();
        response.Errors.Should().ContainSingle();
        response.Errors.First().Should().Be(message);
    }

    protected void AssertBaseErrorMessage<TResponse>(Result<TResponse> response, string message)
    {
        response.Should().NotBeNull();
        response.IsSuccess.Should().BeFalse();
        response.Status.Should().Be(ResultStatus.Error);
        response.Errors.Should().NotBeEmpty();
        response.Errors.Should().ContainSingle();
        response.Errors.First().Should().Be(message);
    }


    #endregion
}