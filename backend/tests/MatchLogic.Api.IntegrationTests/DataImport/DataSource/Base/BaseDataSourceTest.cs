using Ardalis.HttpClientTestExtensions;
using MatchLogic.Api.Endpoints;
using MatchLogic.Api.Handlers.DataProfile.GenerateAdvanceProfile;
using MatchLogic.Api.Handlers.DataSource.Create;
using MatchLogic.Api.IntegrationTests.Builders;
using MatchLogic.Api.IntegrationTests.Common;
using System.Text.Json.Nodes;

namespace MatchLogic.Api.IntegrationTests.DataImport.DataSource.Base;

public class BaseDataSourceTest : BaseApiTest
{

    public BaseDataSourceTest() : base(DataImportEndpoints.PATH)
    {
    }
    internal async Task<List<Domain.Project.DataSource>> AddTestMultipleDataSource()
    {
        var req = await new DataSourceBuilder(GetServiceProvider())
           .AddMultipleDataSourcesAsync(["DataSource1", "DataSource2"]);
        return req.DataSources;        
    }
    internal async Task<Domain.Project.DataSource> AddTestSingleDataSource()
    {
        var req = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync("DataSource1");
        return req.DataSource;
    }

    internal async Task<Domain.Project.DataSource> AddTestSingleDataSource(CreateDataSourceRequest request)
    {
        var req = await new DataSourceBuilder(GetServiceProvider())
            .AddSingleDataSourceAsync(request);
        return req.DataSource;
    }
    internal async Task<List<Domain.Project.DataSource>> AddTestMultipleDataSource(CreateDataSourceRequest request)
    {
        var req = await new DataSourceBuilder(GetServiceProvider())
            .AddMultipleDataSourcesAsync(request);
        return req.DataSources;        
    }

    internal Task<Project> CreateTestProject()
    {
        return new ProjectBuilder(GetServiceProvider())
            .BuildAsync();
    }
    

    internal CreateDataSourceRequest CreateDataSourceRequest(Guid projectId, BaseConnectionInfo connectionInfo, List<DataSourceRequest> dataSources)
    {
        return new CreateDataSourceRequest
        (
            ProjectId: projectId,
            Connection: connectionInfo,
            DataSources: dataSources
        );
    }


    public JsonObject ConvertIntoJsonObject<TConnection>(TConnection connectionInfo)
    {
        var requestObj = new JsonObject();

        if(connectionInfo == null)
            throw new ArgumentNullException(nameof(connectionInfo));    

        // Convert connection info to JsonObject
        foreach (var property in connectionInfo.GetType().GetProperties())
        {
            var value = property.GetValue(connectionInfo);
            if (value != null)
            {
                requestObj[property.Name] = JsonValue.Create(value);
            }
        }

        return requestObj;
    }


    private protected async Task<Result<TResponse>> SendRequest<TResponse>(string? uri, string SourceType, object request)
    {
        string requestUri = uri ?? "";
        string url = $"{RequestURIPath}{requestUri}?SourceType={SourceType}";
        return await httpClient.PostAndDeserializeAsync<Result<TResponse>>(url,
            StringContentHelpers.FromModelAsJson(request));
    }

    private protected async Task<Result<TResponse>> SendPostRequest<TResponse>(string? uri, BaseConnectionInfo request)
    {
        string requestUri = uri ?? "";
        string url = $"{RequestURIPath}{requestUri}";
        return await httpClient.PostAndDeserializeAsync<Result<TResponse>>(url,
            StringContentHelpers.FromModelAsJson(request));
    }



}



