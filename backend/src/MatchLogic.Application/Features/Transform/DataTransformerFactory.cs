using MatchLogic.Application.Interfaces.Transform;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace MatchLogic.Application.Features.Transform;

/// <summary>
/// Registry and factory for data transformers.
/// Supports automatic discovery via [HandlesDataTransformer] attribute.
/// Mirrors ExportDataWriterStrategyFactory pattern.
/// </summary>
public interface IDataTransformerFactory
{
    /// <summary>
    /// Get transformer by type key and configuration.
    /// Returns no-op if type is null/"none".
    /// Throws if type is unknown and no fallback registered.
    /// </summary>
    IDataTransformer GetTransformer(TransformerConfiguration config);

    /// <summary>
    /// Register custom transformer at runtime.
    /// Allows extending without modifying existing code.
    /// </summary>
    void RegisterTransformer(string key, Func<TransformerConfiguration, ILogger?, IDataTransformer> factory);
}

/// <summary>
/// Built-in factory implementation with automatic discovery.
/// PERFORMANCE: Discovery happens once at construction; lookups are O(1) dictionary access.
/// </summary>
public class DataTransformerFactory : IDataTransformerFactory
{
    /// <summary>
    /// Map of transformer key → transformer type.
    /// Discovered via reflection at construction time.
    /// </summary>
    private readonly Dictionary<string, Type> _transformerTypeMap;

    /// <summary>
    /// Map of transformer key → factory function.
    /// Allows runtime registration of custom transformers.
    /// Merged with discovered types.
    /// </summary>
    private readonly Dictionary<string, Func<TransformerConfiguration, ILogger?, IDataTransformer>> _transformerFactoryMap;

    private readonly ILogger<DataTransformerFactory> _logger;

    public DataTransformerFactory(ILogger<DataTransformerFactory> logger)
    {
        _logger = logger;

        // Discover transformer types from infrastructure/application assemblies
        // PERFORMANCE: Single reflection pass during construction
        _transformerTypeMap = DiscoverTransformerTypes();

        // Initialize factory map with discovered types
        // PERFORMANCE: Lazy instantiation via reflection
        _transformerFactoryMap = BuildFactoryMap(_transformerTypeMap);

        LogDiscoveredTransformers();
    }

    /// <summary>
    /// Discover all transformer types marked with [HandlesDataTransformer] attribute.
    /// Mirrors ExportDataWriterStrategyFactory.DiscoverStrategyTypes().
    /// </summary>
    private static Dictionary<string, Type> DiscoverTransformerTypes()
    {
#pragma warning disable CS8619 // Nullability of reference types in value doesn't match target type.
        return
            AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && (a.GetName().Name?.Contains("MatchLogic") ?? false))
            .SelectMany(a => a.GetTypes())
            .Where(t => t.IsClass && !t.IsAbstract && typeof(IDataTransformer).IsAssignableFrom(t))
            .Select(t => new
            {
                TransformerType = t,
                Attribute = t.GetCustomAttribute<HandlesDataTransformer>(),
            })
            .Where(x => x.Attribute != null)
            .ToDictionary(
                x => x.Attribute!.TransformerKey,
                x => x.TransformerType,
                StringComparer.OrdinalIgnoreCase
            );
#pragma warning restore CS8619 // Nullability of reference types in value doesn't match target type.
    }

    /// <summary>
    /// Build factory map from discovered types.
    /// Creates factory functions that use reflection to instantiate transformers.
    /// </summary>
    private static Dictionary<string, Func<TransformerConfiguration, ILogger?, IDataTransformer>> BuildFactoryMap(
        Dictionary<string, Type> typeMap)
    {
        var factoryMap = new Dictionary<string, Func<TransformerConfiguration, ILogger?, IDataTransformer>>(
            StringComparer.OrdinalIgnoreCase);

        foreach (var (key, transformerType) in typeMap)
        {
            // Capture type in closure for factory function
            var type = transformerType;

            // Factory function: create instance via reflection
            // PERFORMANCE: Single allocation per call (unavoidable for flexibility)
            factoryMap[key] = (config, logger) =>
            {
                try
                {
                    // Try constructor with (TransformerConfiguration, ILogger)
                    var ctor = type.GetConstructor(
                        new[] { typeof(TransformerConfiguration), typeof(ILogger) });

                    if (ctor != null)
                    {
                        return (IDataTransformer)ctor.Invoke(new object?[] { config, logger });
                    }

                    // Fallback: constructor with (TransformerConfiguration)
                    ctor = type.GetConstructor(new[] { typeof(TransformerConfiguration) });
                    if (ctor != null)
                    {
                        return (IDataTransformer)ctor.Invoke(new object[] { config });
                    }

                    throw new InvalidOperationException(
                        $"Transformer {type.Name} must have constructor accepting TransformerConfiguration");
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Failed to instantiate transformer {type.Name}", ex);
                }
            };
        }

        return factoryMap;
    }

    public IDataTransformer GetTransformer(TransformerConfiguration config)
    {
        if (config == null)
            throw new ArgumentNullException(nameof(config));

        config.Validate();

        // Normalize type key (handle null/"none" as identity)
        var typeKey = string.IsNullOrWhiteSpace(config.TransformerType)
            ? "none"
            : config.TransformerType;

        if (!_transformerFactoryMap.TryGetValue(typeKey, out var factory))
        {
            _logger.LogWarning(
                "Unknown transformer type: {TransformerType}. Falling back to no-op.",
                config.TransformerType);

            // Fallback to no-op transformer with same column projections
            return _transformerFactoryMap["none"](
                new TransformerConfiguration
                {
                    TransformerType = "none",
                    Settings = config.Settings,
                    ColumnProjections = config.ColumnProjections,
                    EnableTracing = config.EnableTracing
                },
                _logger);
        }

        try
        {
            return factory(config, _logger);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Error creating transformer {TransformerType} with config",
                config.TransformerType);
            throw;
        }
    }

    public void RegisterTransformer(
        string key,
        Func<TransformerConfiguration, ILogger?, IDataTransformer> factory)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("Transformer key cannot be null or empty", nameof(key));

        if (factory == null)
            throw new ArgumentNullException(nameof(factory));

        if (_transformerFactoryMap.ContainsKey(key))
        {
            _logger.LogWarning(
                "Overwriting transformer registration for key: {Key}",
                key);
        }

        _transformerFactoryMap[key] = factory;
    }

    private void LogDiscoveredTransformers()
    {
        var count = _transformerTypeMap.Count;
        var keys = string.Join(", ", _transformerTypeMap.Keys.OrderBy(k => k));
        _logger.LogInformation(
            "DataTransformerFactory discovered {Count} transformer(s): [{Keys}]",
            count,
            keys);
    }
}