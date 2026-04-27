using Azure.Storage.Blobs;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;

[CollectionDefinition("Azurite")]
public class AzuriteCollection : ICollectionFixture<AzuriteContainerFixture> { }

public class AzuriteContainerFixture : IAsyncLifetime
{
    private IContainer _container = null!;

    private const string AccountName = "devstoreaccount1";
    private const string AccountKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(10000);

    public string ConnectionString =>
        $"DefaultEndpointsProtocol=http;AccountName={AccountName};AccountKey={AccountKey};" +
        $"BlobEndpoint=http://{Host}:{Port}/{AccountName};";

    public string ContainerName => "test-container";

    public async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("mcr.microsoft.com/azure-storage/azurite")
            .WithCommand("azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000")
            .WithPortBinding(10000, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(10000))
            .Build();

        await _container.StartAsync();

        // Create the test container using Azure.Storage.Blobs
        var blobServiceClient = new BlobServiceClient(ConnectionString);
        await blobServiceClient.CreateBlobContainerAsync(ContainerName);
    }

    public async Task DisposeAsync()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
