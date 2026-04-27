using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Infrastructure.Core.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Application.UnitTests;
public class SystemResourcesHealthCheckTests
{
    private class TestFileSystem : IFileSystem
    {
        public string MemInfoContent { get; set; } = "";
        public string[] StatContent { get; set; } = Array.Empty<string>();
        public int StatCallCount { get; private set; }
        public bool ThrowError { get; set; }

        public string ReadAllText(string path)
        {
            if (ThrowError)
                throw new IOException("Simulated file system error");

            if (path == "/proc/meminfo")
                return MemInfoContent;
            throw new FileNotFoundException();
        }

        public string[] ReadAllLines(string path)
        {
            if (ThrowError)
                throw new IOException("Simulated file system error");

            if (path == "/proc/stat")
            {
                StatCallCount++;
                return StatContent;
            }
            throw new FileNotFoundException();
        }
    }

    private readonly TestFileSystem _fileSystem;
    private readonly RecordLinkageOptions _options;
    private readonly Mock<ILogger<SystemResourcesHealthCheck>> _logger;
    private readonly TestSystemResourcesHealthCheck _healthCheck;

    public SystemResourcesHealthCheckTests()
    {
        _fileSystem = new TestFileSystem();
        _options = new RecordLinkageOptions
        {
            MaxMemoryMB = 1000,
            MaxDegreeOfParallelism = 4
        };
        _logger = new Mock<ILogger<SystemResourcesHealthCheck>>();
        var optionsWrapper = new OptionsWrapper<RecordLinkageOptions>(_options);

        _healthCheck = new TestSystemResourcesHealthCheck(
            optionsWrapper,
            _logger,
            _fileSystem);
    }

    [Fact]
    public async Task CheckHealthAsync_WhenHealthy_ReturnsHealthy()
    {
        // Arrange
        SetupHealthySystemConditions();

        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Contains("System resources are healthy", result.Description);
        Assert.NotNull(result.Data);
        Assert.Equal(3, result.Data.Count);
    }

    [Fact]
    public async Task CheckHealthAsync_WithLowMemory_ReturnsDegraded()
    {
        // Arrange
        SetupLowMemoryConditions();

        //var options = new RecordLinkageOptions
        //{
        //    MaxMemoryMB = 1000,
        //    MaxDegreeOfParallelism = 4
        //};
        //var optionsWrapper = new OptionsWrapper<RecordLinkageOptions>(options);
        //var logger = new Mock<ILogger<SystemResourcesHealthCheck>>();
        //var fileSystem = new TestFileSystem
        //{
        //    ThrowError = true // Ensure file system operations throw errors
        //};

        //var healthCheck = new TestSystemResourcesHealthCheck(
        //    optionsWrapper,
        //    logger,
        //    fileSystem);
        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("Low memory availability", result.Description);
        var availableMemory = Convert.ToInt64(result.Data["AvailableMemoryMB"]);
        Assert.True(availableMemory < _options.MaxMemoryMB * 0.2);
    }

    [Fact]
    public async Task CheckHealthAsync_WithHighCpuUsage_ReturnsDegraded()
    {
        // Arrange
        SetupHighCpuUsageConditions();

        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("High CPU usage", result.Description);
        var cpuUsage = Convert.ToDouble(result.Data["CpuUsage"]);
        Assert.True(cpuUsage > 85);
    }

    [Fact]
    public async Task CheckHealthAsync_WithError_ReturnsUnhealthy()
    {
        // Arrange
        SetupErrorConditions();

        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("Error checking system resources", result.Description);
    }

    [Fact]
    public async Task CheckHealthAsync_OnLinux_ReadsFromProcFS()
    {
        // Arrange
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return; // Skip on non-Linux platforms
        }

        SetupLinuxSystemConditions();

        // Act
        var result = await _healthCheck.CheckHealthAsync(new HealthCheckContext());

        // Assert
        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Equal(2, _fileSystem.StatCallCount); // Called twice for CPU measurement
    }

    [Fact]
    public async Task CheckHealthAsync_WithCancellation_Cancels()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await _healthCheck.CheckHealthAsync(new HealthCheckContext(), cts.Token));
    }

    private void SetupHealthySystemConditions()
    {
        // Simulate healthy system conditions
        _fileSystem.MemInfoContent = "MemTotal: 16000000 kB\nMemAvailable: 8000000 kB\n";
        _fileSystem.StatContent = new[]
        {
            "cpu  2000 500 1000 8000 200 0 100 0 0 0",
            "cpu0 500 100 200 2000 50 0 25 0 0 0"
        };
    }

    private void SetupLowMemoryConditions()
    {
        // Simulate low memory conditions
        _fileSystem.MemInfoContent = "MemTotal: 16000000 kB\nMemAvailable: 100000 kB\n";
        _fileSystem.StatContent = new[]
        {
            "cpu  2000 500 1000 8000 200 0 100 0 0 0",
            "cpu0 500 100 200 2000 50 0 25 0 0 0"
        };
    }

    private void SetupHighCpuUsageConditions()
    {
        // Set healthy memory conditions
        _fileSystem.MemInfoContent = "MemTotal: 16000000 kB\nMemAvailable: 8000000 kB\n";

        // Set CPU stats that will result in high usage
        _fileSystem.StatContent = new[]
        {
            "cpu  9000 500 1000 500 200 0 100 0 0 0"
        };

        // Force high CPU usage through the test implementation
        _healthCheck.SetCpuUsageOverride(90.0);
    }

    private void SetupErrorConditions()
    {
        // Simulate system error conditions
        _fileSystem.ThrowError = true;
        _fileSystem.MemInfoContent = "Invalid Content";
        _fileSystem.StatContent = new[] { "Invalid Content" };
    }

    private void SetupLinuxSystemConditions()
    {
        _fileSystem.MemInfoContent = "MemTotal: 16000000 kB\nMemAvailable: 8000000 kB\n";
        _fileSystem.StatContent = new[]
        {
            "cpu  2000 500 1000 8000 200 0 100 0 0 0",
            "cpu0 500 100 200 2000 50 0 25 0 0 0"
        };
    }
}
public interface IFileSystem
{
    string ReadAllText(string path);
    string[] ReadAllLines(string path);
}

public class TestSystemResourcesHealthCheck : SystemResourcesHealthCheck
{
    private readonly IFileSystem _fileSystem;
    private double? _cpuUsageOverride;

    public TestSystemResourcesHealthCheck(
        IOptions<RecordLinkageOptions> options,
        Mock<ILogger<SystemResourcesHealthCheck>> logger,
        IFileSystem fileSystem,
        double? cpuUsageOverride = null)
        : base(options, logger.Object)
    {
        _fileSystem = fileSystem;
        _cpuUsageOverride = cpuUsageOverride;
    }

    protected override string ReadAllText(string path) => _fileSystem.ReadAllText(path);
    protected override string[] ReadAllLines(string path) => _fileSystem.ReadAllLines(path);

    protected override bool IsWindowsPlatform()
    {
        return false;
    }
    protected override async Task<double> GetCpuUsageAsync()
    {
        if (_cpuUsageOverride.HasValue)
        {
            return _cpuUsageOverride.Value;
        }

        // If no override, call the base implementation
        return await base.GetCpuUsageAsync();
    }
    public void SetCpuUsageOverride(double value)
    {
        _cpuUsageOverride = value;
    }
}