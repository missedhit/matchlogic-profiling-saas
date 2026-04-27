using MatchLogic.Application.Features.DataMatching.RecordLinkage;
using MatchLogic.Application.Features.DataMatching.RecordLinkageg;
using MatchLogic.Application.Features.DataMatching.Storage;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Infrastructure;
using MatchLogic.Infrastructure.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace MatchLogic.Application.UnitTests;

/// <summary>
/// Comprehensive unit tests for MatchGraphStorage
/// Uses actual LiteDB datastore instead of mocking
/// </summary>
public class MatchGraphStorageTests : IDisposable, IAsyncLifetime
{
    private readonly string _dbPath;
    private readonly IDataStore _dataStore;
    private readonly MatchGraphStorage _graphStorage;
    private readonly ILogger<MatchGraphStorage> _logger;
    private readonly IServiceProvider _serviceProvider;
    private const string TEST_COLLECTION = "test_matchgraph";

    public MatchGraphStorageTests()
    {
        // Create temporary database file
        _dbPath = Path.GetTempFileName();

        // Setup logging
        using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddConsole().SetMinimumLevel(LogLevel.Information));
        _logger = loggerFactory.CreateLogger<MatchGraphStorage>();

        // Create service collection
        var services = new ServiceCollection();

        // Build configuration
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                { "ConnectionStrings:LiteDB", _dbPath }
            })
            .Build();

        services.AddLogging(builder => builder.AddConsole());
        services.AddSingleton<IConfiguration>(config);
        services.AddApplicationSetup(_dbPath);
        

        // Build service provider
        _serviceProvider = services.BuildServiceProvider();

        // Get instances
        _dataStore = _serviceProvider.GetRequiredService<IDataStore>();
        _graphStorage = new MatchGraphStorage(_logger);
    }

    #region Basic Save and Load Tests

    [Fact]
    public async Task SaveMatchGraphAsync_WithValidGraph_ShouldSaveSuccessfully()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 100, edgesPerNode: 3);
        var collectionName = $"{TEST_COLLECTION}_save_valid";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);

        // Assert
        var exists = await _graphStorage.GraphExistsAsync(_dataStore, collectionName);
        Assert.True(exists);
    }

    [Fact]
    public async Task LoadMatchGraphAsync_AfterSave_ShouldReturnIdenticalGraph()
    {
        // Arrange
        var originalGraph = CreateTestGraph(nodeCount: 100, edgesPerNode: 3);
        var collectionName = $"{TEST_COLLECTION}_load_identical";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, originalGraph);
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert
        Assert.NotNull(loadedGraph);
        Assert.Equal(originalGraph.TotalNodes, loadedGraph.TotalNodes);
        Assert.Equal(originalGraph.TotalEdges, loadedGraph.TotalEdges);
        Assert.Equal(originalGraph.GraphId, loadedGraph.GraphId);
        Assert.Equal(originalGraph.ProjectId, loadedGraph.ProjectId);
    }

    [Fact]
    public async Task LoadMatchGraphAsync_WithNonExistentCollection_ShouldReturnNull()
    {
        // Arrange
        var collectionName = $"{TEST_COLLECTION}_nonexistent";

        // Act
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert
        Assert.Null(loadedGraph);
    }

    #endregion

    #region Data Integrity Tests

    [Fact]
    public async Task SaveAndLoad_ShouldPreserveAdjacencyList()
    {
        // Arrange
        var originalGraph = CreateTestGraph(nodeCount: 50, edgesPerNode: 2);
        var collectionName = $"{TEST_COLLECTION}_adjacency";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, originalGraph);
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert
        Assert.Equal(originalGraph.AdjacencyList.Count, loadedGraph.AdjacencyList.Count);

        // Verify each node's neighbors
        foreach (var kvp in originalGraph.AdjacencyList)
        {
            Assert.True(loadedGraph.AdjacencyList.ContainsKey(kvp.Key));
            var originalNeighbors = kvp.Value.OrderBy(n => n.ToString()).ToList();
            var loadedNeighbors = loadedGraph.AdjacencyList[kvp.Key].OrderBy(n => n.ToString()).ToList();
            Assert.Equal(originalNeighbors.Count, loadedNeighbors.Count);

            for (int i = 0; i < originalNeighbors.Count; i++)
            {
                Assert.Equal(originalNeighbors[i], loadedNeighbors[i]);
            }
        }
    }

    [Fact]
    public async Task SaveAndLoad_ShouldPreserveEdgeDetails()
    {
        // Arrange
        var originalGraph = CreateTestGraph(nodeCount: 50, edgesPerNode: 2);
        var collectionName = $"{TEST_COLLECTION}_edges";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, originalGraph);
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert
        Assert.Equal(originalGraph.EdgeDetails.Count, loadedGraph.EdgeDetails.Count);

        // Verify each edge's details
        foreach (var kvp in originalGraph.EdgeDetails)
        {
            Assert.True(loadedGraph.EdgeDetails.ContainsKey(kvp.Key));
            var originalEdge = kvp.Value;
            var loadedEdge = loadedGraph.EdgeDetails[kvp.Key];

            Assert.Equal(originalEdge.PairId, loadedEdge.PairId);
            Assert.Equal(originalEdge.MaxScore, loadedEdge.MaxScore);
            Assert.Equal(originalEdge.MatchDefinitionIndices.Count, loadedEdge.MatchDefinitionIndices.Count);
            Assert.Equal(originalEdge.MatchedAt.ToString("yyyy-MM-dd HH:mm:ss"),
                        loadedEdge.MatchedAt.ToString("yyyy-MM-dd HH:mm:ss"));
        }
    }

    [Fact]
    public async Task SaveAndLoad_ShouldPreserveNodeMetadata()
    {
        // Arrange
        var originalGraph = CreateTestGraph(nodeCount: 50, edgesPerNode: 2);
        var collectionName = $"{TEST_COLLECTION}_metadata";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, originalGraph);
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert
        Assert.Equal(originalGraph.NodeMetadata.Count, loadedGraph.NodeMetadata.Count);

        // Verify node metadata
        foreach (var kvp in originalGraph.NodeMetadata)
        {
            Assert.True(loadedGraph.NodeMetadata.ContainsKey(kvp.Key));
            var originalMeta = kvp.Value;
            var loadedMeta = loadedGraph.NodeMetadata[kvp.Key];

            Assert.Equal(originalMeta.RecordKey, loadedMeta.RecordKey);
            Assert.Equal(originalMeta.DegreeCount, loadedMeta.DegreeCount);
            Assert.Equal(originalMeta.RecordData.Count, loadedMeta.RecordData.Count);
        }
    }

    [Fact]
    public async Task SaveAndLoad_ScoresByDefinition_ShouldBeExcluded()
    {
        // Arrange
        var originalGraph = CreateTestGraph(nodeCount: 50, edgesPerNode: 2);

        // Add ScoresByDefinition to edges
        foreach (var edge in originalGraph.EdgeDetails.Values)
        {
            edge.ScoresByDefinition = new Dictionary<int, MatchScoreDetail>
            {
                { 0, new MatchScoreDetail { FinalScore = 0.95 } },
                { 1, new MatchScoreDetail { FinalScore = 0.80 } }
            };
        }

        var collectionName = $"{TEST_COLLECTION}_no_scores";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, originalGraph);
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert - ScoresByDefinition should be empty due to [IgnoreMember]
        foreach (var edge in loadedGraph.EdgeDetails.Values)
        {
            Assert.NotNull(edge.ScoresByDefinition);
            Assert.Empty(edge.ScoresByDefinition);
        }
    }

    #endregion

    #region Size and Performance Tests

    [Fact]
    public async Task SaveMatchGraphAsync_SmallGraph_ShouldCompressEffectively()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 100, edgesPerNode: 2);
        var collectionName = $"{TEST_COLLECTION}_small";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var metadata = await _graphStorage.GetGraphMetadataAsync(_dataStore, collectionName);

        // Assert
        Assert.NotNull(metadata);
        Assert.True(metadata.CompressedSizeBytes < metadata.UncompressedSizeBytes);
        Assert.True(metadata.CompressionRatio < 1.0); // Should be compressed

        _logger.LogInformation(
            "Small graph compression: {Uncompressed} -> {Compressed} ({Ratio:P})",
            metadata.UncompressedSizeBytes,
            metadata.CompressedSizeBytes,
            metadata.CompressionRatio);
    }

    [Fact]
    public async Task SaveMatchGraphAsync_MediumGraph_ShouldHandleEfficiently()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 1000, edgesPerNode: 3);
        var collectionName = $"{TEST_COLLECTION}_medium";

        // Act
        var startTime = DateTime.UtcNow;
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var saveTime = DateTime.UtcNow - startTime;

        startTime = DateTime.UtcNow;
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);
        var loadTime = DateTime.UtcNow - startTime;

        // Assert
        Assert.NotNull(loadedGraph);
        Assert.True(saveTime.TotalSeconds < 5, $"Save took {saveTime.TotalSeconds}s, expected < 5s");
        Assert.True(loadTime.TotalSeconds < 5, $"Load took {loadTime.TotalSeconds}s, expected < 5s");

        _logger.LogInformation(
            "Medium graph ({Nodes} nodes, {Edges} edges): Save={SaveMs}ms, Load={LoadMs}ms",
            graph.TotalNodes,
            graph.TotalEdges,
            saveTime.TotalMilliseconds,
            loadTime.TotalMilliseconds);
    }

    [Fact]
    public async Task SaveMatchGraphAsync_LargeGraph_ShouldNotExceedLiteDBLimit()
    {
        // Arrange - Simulating larger graph
        var graph = CreateTestGraph(nodeCount: 5000, edgesPerNode: 4);
        var collectionName = $"{TEST_COLLECTION}_large";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var metadata = await _graphStorage.GetGraphMetadataAsync(_dataStore, collectionName);

        // Assert - LiteDB has a ~16MB document limit; compressed should be well below
        var maxLiteDBSize = 16 * 1024 * 1024; // 16 MB
        Assert.True(metadata.CompressedSizeBytes < maxLiteDBSize,
            $"Compressed size {metadata.CompressedSizeBytes:N0} bytes exceeds LiteDB limit");

        _logger.LogInformation(
            "Large graph ({Nodes} nodes, {Edges} edges): Compressed to {Size} ({Percentage} of 16MB limit)",
            graph.TotalNodes,
            graph.TotalEdges,
            metadata.CompressedSizeMB,
            (metadata.CompressedSizeBytes / (double)maxLiteDBSize).ToString("P1"));
    }

    [Fact]
    public async Task SaveMatchGraphAsync_VeryLargeGraph_ShouldDemonstrateCompression()
    {
        // Arrange - This would represent ~100K records with ~500K edges
        var graph = CreateTestGraph(nodeCount: 10000, edgesPerNode: 5);
        var collectionName = $"{TEST_COLLECTION}_very_large";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var metadata = await _graphStorage.GetGraphMetadataAsync(_dataStore, collectionName);

        // Assert - Should achieve significant compression
        var compressionPercentage = (1 - metadata.CompressionRatio) * 100;
        Assert.True(compressionPercentage > 50,
            $"Expected > 50% compression, got {compressionPercentage:F1}%");

        _logger.LogInformation(
            "Very large graph: {Nodes:N0} nodes, {Edges:N0} edges\n" +
            "  Uncompressed: {Uncompressed}\n" +
            "  Compressed: {Compressed}\n" +
            "  Savings: {Savings}",
            graph.TotalNodes,
            graph.TotalEdges,
            metadata.UncompressedSizeMB,
            metadata.CompressedSizeMB,
            metadata.CompressionPercentage);
    }

    #endregion

    #region Edge Cases Tests

    [Fact]
    public async Task SaveMatchGraphAsync_EmptyGraph_ShouldSaveAndLoad()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var emptyGraph = new MatchGraphDME(projectId, 0);
        var collectionName = $"{TEST_COLLECTION}_empty";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, emptyGraph);
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert
        Assert.NotNull(loadedGraph);
        Assert.Equal(0, loadedGraph.TotalNodes);
        Assert.Equal(0, loadedGraph.TotalEdges);
    }


    [Fact]
    public async Task SaveMatchGraphAsync_GraphWithComplexRecordData_ShouldPreserveData()
    {
        // Arrange
        var projectId = Guid.NewGuid();
        var graph = new MatchGraphDME(projectId, 1);
        var node = new RecordKey(Guid.NewGuid(), 1);

        // Complex record data with various types
        var recordData = new Dictionary<string, object>
        {
            ["Id"] = 123,
            ["Name"] = "Test Record",
            ["Score"] = 95.5,
            ["IsActive"] = true,
            ["Tags"] = new List<string> { "tag1", "tag2", "tag3" },
            ["CreatedDate"] = DateTime.UtcNow,
            ["Metadata"] = new Dictionary<string, object>
            {
                ["Source"] = "Import",
                ["Version"] = 2
            }
        };

        graph.AddNode(node, recordData);
        graph.UpdateNodeDegrees();

        var collectionName = $"{TEST_COLLECTION}_complex_data";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);

        // Assert
        Assert.True(loadedGraph.NodeMetadata.ContainsKey(node));
        var loadedData = loadedGraph.NodeMetadata[node].RecordData;

        Assert.Equal(123, Convert.ToInt32(loadedData["Id"]));
        Assert.Equal("Test Record", loadedData["Name"]);
        Assert.True(Convert.ToBoolean(loadedData["IsActive"]));
    }

    #endregion

    #region GraphExists Tests

    [Fact]
    public async Task GraphExistsAsync_WithExistingGraph_ShouldReturnTrue()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 50, edgesPerNode: 2);
        var collectionName = $"{TEST_COLLECTION}_exists_true";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var exists = await _graphStorage.GraphExistsAsync(_dataStore, collectionName);

        // Assert
        Assert.True(exists);
    }

    [Fact]
    public async Task GraphExistsAsync_WithNonExistentGraph_ShouldReturnFalse()
    {
        // Arrange
        var collectionName = $"{TEST_COLLECTION}_exists_false";

        // Act
        var exists = await _graphStorage.GraphExistsAsync(_dataStore, collectionName);

        // Assert
        Assert.False(exists);
    }

    [Fact]
    public async Task GraphExistsAsync_AfterSaveAndDelete_ShouldReturnFalse()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 50, edgesPerNode: 2);
        var collectionName = $"{TEST_COLLECTION}_exists_deleted";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var existsBefore = await _graphStorage.GraphExistsAsync(_dataStore, collectionName);

        await _dataStore.DeleteCollection(collectionName);
        var existsAfter = await _graphStorage.GraphExistsAsync(_dataStore, collectionName);

        // Assert
        Assert.True(existsBefore);
        Assert.False(existsAfter);
    }

    #endregion

    #region Metadata Tests

    [Fact]
    public async Task GetGraphMetadataAsync_ShouldReturnAccurateMetadata()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 200, edgesPerNode: 3);
        var collectionName = $"{TEST_COLLECTION}_metadata_accurate";

        // Act
        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);
        var metadata = await _graphStorage.GetGraphMetadataAsync(_dataStore, collectionName);

        // Assert
        Assert.NotNull(metadata);
        Assert.Equal(graph.GraphId, metadata.GraphId);
        Assert.Equal(graph.ProjectId, metadata.ProjectId);
        Assert.Equal(graph.TotalNodes, metadata.TotalNodes);
        Assert.Equal(graph.TotalEdges, metadata.TotalEdges);
        Assert.True(metadata.CompressedSizeBytes > 0);
        Assert.True(metadata.UncompressedSizeBytes > 0);
        Assert.True(metadata.CompressedSizeBytes < metadata.UncompressedSizeBytes);
    }

    [Fact]
    public async Task GetGraphMetadataAsync_WithNonExistentGraph_ShouldReturnNull()
    {
        // Arrange
        var collectionName = $"{TEST_COLLECTION}_metadata_null";

        // Act
        var metadata = await _graphStorage.GetGraphMetadataAsync(_dataStore, collectionName);

        // Assert
        Assert.Null(metadata);
    }

    [Fact]
    public async Task GetGraphMetadataAsync_ShouldNotLoadFullGraph()
    {
        // Arrange - Create large graph
        var graph = CreateTestGraph(nodeCount: 5000, edgesPerNode: 4);
        var collectionName = $"{TEST_COLLECTION}_metadata_fast";

        await _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graph);

        // Act - Metadata should be much faster than full load
        var metadataStart = DateTime.UtcNow;
        var metadata = await _graphStorage.GetGraphMetadataAsync(_dataStore, collectionName);
        var metadataTime = DateTime.UtcNow - metadataStart;

        var loadStart = DateTime.UtcNow;
        var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);
        var loadTime = DateTime.UtcNow - loadStart;

        // Assert
        Assert.NotNull(metadata);
        Assert.NotNull(loadedGraph);

        // Metadata retrieval should be significantly faster
        Assert.True(metadataTime < loadTime,
            $"Metadata ({metadataTime.TotalMilliseconds}ms) should be faster than full load ({loadTime.TotalMilliseconds}ms)");

        _logger.LogInformation(
            "Metadata: {MetadataMs}ms, Full Load: {LoadMs}ms (Metadata is {Ratio:F1}x faster)",
            metadataTime.TotalMilliseconds,
            loadTime.TotalMilliseconds,
            loadTime.TotalMilliseconds / metadataTime.TotalMilliseconds);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task SaveMatchGraphAsync_WithNullDataStore_ShouldThrowArgumentNullException()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 10, edgesPerNode: 2);
        var collectionName = $"{TEST_COLLECTION}_null_datastore";

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            _graphStorage.SaveMatchGraphAsync(null, collectionName, graph));
    }

    [Fact]
    public async Task SaveMatchGraphAsync_WithNullCollectionName_ShouldThrowArgumentException()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 10, edgesPerNode: 2);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            _graphStorage.SaveMatchGraphAsync(_dataStore, null, graph));
    }

    [Fact]
    public async Task SaveMatchGraphAsync_WithEmptyCollectionName_ShouldThrowArgumentException()
    {
        // Arrange
        var graph = CreateTestGraph(nodeCount: 10, edgesPerNode: 2);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            _graphStorage.SaveMatchGraphAsync(_dataStore, "", graph));
    }

    [Fact]
    public async Task SaveMatchGraphAsync_WithNullGraph_ShouldThrowArgumentNullException()
    {
        // Arrange
        var collectionName = $"{TEST_COLLECTION}_null_graph";

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            _graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, null));
    }

    [Fact]
    public async Task LoadMatchGraphAsync_WithNullDataStore_ShouldThrowArgumentNullException()
    {
        // Arrange
        var collectionName = $"{TEST_COLLECTION}_load_null_datastore";

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            _graphStorage.LoadMatchGraphAsync(null, collectionName));
    }

    [Fact]
    public async Task LoadMatchGraphAsync_WithNullCollectionName_ShouldThrowArgumentException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() =>
            _graphStorage.LoadMatchGraphAsync(_dataStore, null));
    }

    #endregion

    #region Concurrency Tests

    [Fact]
    public async Task SaveMatchGraphAsync_ConcurrentSaves_ShouldHandleGracefully()
    {
        // Arrange
        var graphs = Enumerable.Range(0, 5)
            .Select(i => CreateTestGraph(nodeCount: 100, edgesPerNode: 2))
            .ToList();

        var tasks = new List<Task>();

        // Act - Save multiple graphs concurrently
        for (int i = 0; i < graphs.Count; i++)
        {
            var index = i;
            var collectionName = $"{TEST_COLLECTION}_concurrent_{index}";
            tasks.Add(_graphStorage.SaveMatchGraphAsync(_dataStore, collectionName, graphs[index]));
        }

        await Task.WhenAll(tasks);

        // Assert - All graphs should be saved successfully
        for (int i = 0; i < graphs.Count; i++)
        {
            var collectionName = $"{TEST_COLLECTION}_concurrent_{i}";
            var exists = await _graphStorage.GraphExistsAsync(_dataStore, collectionName);
            Assert.True(exists, $"Graph {i} should exist");

            var loadedGraph = await _graphStorage.LoadMatchGraphAsync(_dataStore, collectionName);
            Assert.NotNull(loadedGraph);
            Assert.Equal(graphs[i].TotalNodes, loadedGraph.TotalNodes);
        }
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Creates a test graph with specified number of nodes and edges per node
    /// </summary>
    private MatchGraphDME CreateTestGraph(int nodeCount, int edgesPerNode)
    {
        var projectId = Guid.NewGuid();
        var graph = new MatchGraphDME(projectId, nodeCount);
        var random = new Random(42); // Fixed seed for reproducibility
        var dataSourceId = Guid.NewGuid();

        // Create nodes
        var nodes = new List<RecordKey>();
        for (int i = 0; i < nodeCount; i++)
        {
            var recordKey = new RecordKey(dataSourceId, i);
            var recordData = new Dictionary<string, object>
            {
                ["Id"] = i,
                ["Name"] = $"Record_{i}",
                ["Email"] = $"user{i}@example.com",
                ["Score"] = random.Next(50, 100),
                ["IsActive"] = random.Next(0, 2) == 1,
                ["CreatedDate"] = DateTime.UtcNow.AddDays(-random.Next(0, 365))
            };

            graph.AddNode(recordKey, recordData);
            nodes.Add(recordKey);
        }

        // Create edges
        long edgeId = 0;
        for (int i = 0; i < nodeCount; i++)
        {
            var sourceNode = nodes[i];

            // Connect to random nodes
            for (int j = 0; j < edgesPerNode && j < nodeCount; j++)
            {
                var targetIndex = (i + j + 1) % nodeCount;
                if (targetIndex == i) continue; // Skip self-loops

                var targetNode = nodes[targetIndex];

                var edgeDetails = new MatchEdgeDetails
                {
                    PairId = edgeId++,
                    MaxScore = random.NextDouble() * 0.5 + 0.5, // 0.5 to 1.0
                    MatchDefinitionIndices = new List<int> { random.Next(0, 3) },
                    MatchedAt = DateTime.UtcNow,
                    // Note: ScoresByDefinition will be automatically excluded by [IgnoreMember]
                    ScoresByDefinition = new Dictionary<int, MatchScoreDetail>()
                };

                graph.AddEdge(sourceNode, targetNode, edgeDetails);

                // Update participating definitions
                if (graph.NodeMetadata.TryGetValue(sourceNode, out var meta1))
                {
                    foreach (var defIndex in edgeDetails.MatchDefinitionIndices)
                        meta1.ParticipatingDefinitions.Add(defIndex);
                }

                if (graph.NodeMetadata.TryGetValue(targetNode, out var meta2))
                {
                    foreach (var defIndex in edgeDetails.MatchDefinitionIndices)
                        meta2.ParticipatingDefinitions.Add(defIndex);
                }
            }
        }

        // Update degrees
        graph.UpdateNodeDegrees();

        return graph;
    }

    #endregion

    #region Cleanup

    public void Dispose()
    {
        _dataStore?.Dispose();

        // Delete temporary database file
        if (File.Exists(_dbPath))
        {
            try
            {
                File.Delete(_dbPath);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    public Task InitializeAsync()
    {
        // Any async initialization can go here
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    #endregion
}