using System;

namespace MatchLogic.Application.Features.Transform;

/// <summary>
/// Marks a class as a discoverable data transformer.
/// Used by DataTransformerFactory to auto-register transformers via reflection.
/// 
/// Pattern mirrors HandlesExportWriter and HandlesConnectionConfig.
/// 
/// Example:
///   [HandlesDataTransformer("flatten")]
///   internal class FlatteningTransformer : BaseDataTransformer { }
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class HandlesDataTransformer : Attribute
{
    /// <summary>
    /// Unique key identifying this transformer type.
    /// Used to look up transformer at runtime.
    /// Examples: "flatten", "projection", "aggregation", "none"
    /// </summary>
    public string TransformerKey { get; }

    public HandlesDataTransformer(string transformerKey)
    {
        if (string.IsNullOrWhiteSpace(transformerKey))
            throw new ArgumentException("Transformer key cannot be null or empty", nameof(transformerKey));

        TransformerKey = transformerKey;
    }
}