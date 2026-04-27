using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;

[CollectionDefinition("SFTP")]
public class SftpCollection : ICollectionFixture<SftpContainerFixture> { }

public class SftpContainerFixture : IAsyncLifetime
{
    private IContainer _container = null!;

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(22);
    public string Username => "testuser";
    public string Password => "testpass";

    public async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("atmoz/sftp")
            .WithCommand("testuser:testpass:::upload")
            .WithPortBinding(22, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(22))
            .Build();

        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
