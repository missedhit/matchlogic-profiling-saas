using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;

[CollectionDefinition("FTP")]
public class FtpCollection : ICollectionFixture<FtpContainerFixture> { }

public class FtpContainerFixture : IAsyncLifetime
{
    private IContainer _container = null!;

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(21);
    public string Username => "testuser";
    public string Password => "testpass";

    /// <summary>
    /// Passive port range used by vsftpd. Fixed host-to-container port mappings
    /// are required so the FTP data channel connects to the correct host port.
    /// PASV_ADDRESS must point to the Docker host (127.0.0.1).
    /// </summary>
    public async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("fauria/vsftpd")
            .WithEnvironment("FTP_USER", "testuser")
            .WithEnvironment("FTP_PASS", "testpass")
            .WithEnvironment("PASV_ENABLE", "YES")
            .WithEnvironment("PASV_ADDRESS", "127.0.0.1")
            .WithEnvironment("PASV_ADDR_RESOLVE", "NO")
            .WithEnvironment("PASV_MIN_PORT", "21100")
            .WithEnvironment("PASV_MAX_PORT", "21110")
            .WithPortBinding(21, true)
            // Fixed port mappings for passive data ports (host == container)
            .WithPortBinding(21100, 21100)
            .WithPortBinding(21101, 21101)
            .WithPortBinding(21102, 21102)
            .WithPortBinding(21103, 21103)
            .WithPortBinding(21104, 21104)
            .WithPortBinding(21105, 21105)
            .WithPortBinding(21106, 21106)
            .WithPortBinding(21107, 21107)
            .WithPortBinding(21108, 21108)
            .WithPortBinding(21109, 21109)
            .WithPortBinding(21110, 21110)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(21))
            .Build();

        await _container.StartAsync();

        // vsftpd needs time to fully initialize after port 21 becomes available.
        // Wait and verify we can actually establish an FTP connection.
        await WaitForFtpReadyAsync();
    }

    private async Task WaitForFtpReadyAsync()
    {
        var maxAttempts = 10;
        for (int i = 0; i < maxAttempts; i++)
        {
            try
            {
                using var client = new System.Net.Sockets.TcpClient();
                await client.ConnectAsync(Host, Port);
                using var stream = client.GetStream();
                stream.ReadTimeout = 3000;
                var buffer = new byte[512];
                var bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                var banner = System.Text.Encoding.ASCII.GetString(buffer, 0, bytesRead);
                if (banner.StartsWith("220"))
                    return; // FTP server is ready
            }
            catch
            {
                // Not ready yet
            }
            await Task.Delay(1000);
        }
    }

    public async Task DisposeAsync()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
