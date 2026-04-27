using MatchLogic.Application.Common;
using MatchLogic.Application.Interfaces.Persistence;
using MatchLogic.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Reflection;

namespace MatchLogic.Infrastructure.Repository;

/// <summary>
/// Resolves the StoreType for a repository class using the following priority:
///   1. StoreSettings:Overrides  (appsettings, by short class name)
///   2. [UseStore] attribute     (compile-time fallback, now optional)
///   3. StoreSettings:Default    (appsettings global default)
///   4. StoreType.MongoDB        (hardcoded safety net)
///
/// Results are cached after first resolution — reflection + config reads happen once per type.
/// </summary>
public sealed class StoreTypeResolver : IStoreTypeResolver
{
    private readonly StoreSettings _settings;
    private readonly ILogger<StoreTypeResolver> _logger;

    // Thread-safe cache: resolved once per repository type for the lifetime of the singleton
    private readonly Dictionary<Type, StoreType> _cache = new();
    private readonly object _lock = new();

    public StoreTypeResolver(
        IOptions<StoreSettings> settings,
        ILogger<StoreTypeResolver> logger)
    {
        _settings = settings.Value;
        _logger = logger;
    }

    public StoreType Resolve(Type repositoryType)
    {
        // Fast path — already resolved
        lock (_lock)
        {
            if (_cache.TryGetValue(repositoryType, out var cached))
                return cached;

            var resolved = ResolveInternal(repositoryType);
            _cache[repositoryType] = resolved;
            return resolved;
        }
    }

    private StoreType ResolveInternal(Type repositoryType)
    {
        var shortName = repositoryType.Name;

        // ── Priority 1: appsettings StoreSettings:Overrides ──────────────────
        if (_settings.Overrides.TryGetValue(shortName, out var overrideValue))
        {
            if (Enum.TryParse<StoreType>(overrideValue, ignoreCase: true, out var overrideStore))
            {
                _logger.LogDebug(
                    "[StoreTypeResolver] {Repository} → {StoreType} (config override)",
                    shortName, overrideStore);
                return overrideStore;
            }

            // Bad value in config — warn and fall through, do not throw (resilient at runtime)
            _logger.LogWarning(
                "[StoreTypeResolver] Config override value '{Value}' for '{Repository}' is not a valid " +
                "StoreType. Valid values: {ValidValues}. Falling through to next priority.",
                overrideValue, shortName, string.Join(", ", Enum.GetNames<StoreType>()));
        }

        // ── Priority 2: [UseStore] attribute (optional, kept for backward compat) ──
        var attribute = repositoryType.GetCustomAttribute<UseStoreAttribute>();
        if (attribute != null)
        {
            _logger.LogDebug(
                "[StoreTypeResolver] {Repository} → {StoreType} ([UseStore] attribute)",
                shortName, attribute.StoreType);
            return attribute.StoreType;
        }

        // ── Priority 3: appsettings StoreSettings:Default ────────────────────
        if (!string.IsNullOrWhiteSpace(_settings.Default) &&
            Enum.TryParse<StoreType>(_settings.Default, ignoreCase: true, out var defaultStore))
        {
            _logger.LogDebug(
                "[StoreTypeResolver] {Repository} → {StoreType} (config default)",
                shortName, defaultStore);
            return defaultStore;
        }

        // ── Priority 4: Hardcoded fallback ────────────────────────────────────
        _logger.LogDebug(
            "[StoreTypeResolver] {Repository} → MongoDB (hardcoded fallback — no config or attribute found)",
            shortName);
        return StoreType.LiteDb;
    }
}