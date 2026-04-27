using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace MatchLogic.Application.UnitTests.RemoteConnectors.Fixtures;

[CollectionDefinition("MinIO")]
public class MinioCollection : ICollectionFixture<MinioContainerFixture> { }

public class MinioContainerFixture : IAsyncLifetime
{
    private IContainer _container = null!;

    public string Host => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(9000);
    public string Endpoint => $"http://{Host}:{Port}";
    public string AccessKey => "minioadmin";
    public string SecretKey => "minioadmin";
    public string BucketName => "test-bucket";

    public async Task InitializeAsync()
    {
        _container = new ContainerBuilder()
            .WithImage("minio/minio")
            .WithCommand("server", "/data")
            .WithEnvironment("MINIO_ROOT_USER", "minioadmin")
            .WithEnvironment("MINIO_ROOT_PASSWORD", "minioadmin")
            .WithPortBinding(9000, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9000))
            .Build();

        await _container.StartAsync();

        // Create the test bucket using the AWS S3 SDK (MinIO requires proper auth)
        var credentials = new BasicAWSCredentials(AccessKey, SecretKey);
        var s3Config = new AmazonS3Config
        {
            ServiceURL = Endpoint,
            ForcePathStyle = true
        };
        using var s3Client = new AmazonS3Client(credentials, s3Config);
        await s3Client.PutBucketAsync(new PutBucketRequest { BucketName = BucketName });
    }

    public async Task DisposeAsync()
    {
        await _container.StopAsync();
        await _container.DisposeAsync();
    }
}
