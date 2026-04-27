using MatchLogic.Domain.Entities;
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

/// <summary>
/// Helper class for converting between MatchGroup domain objects and database documents
/// </summary>
public static class MatchGroupConverter
{
    /// <summary>
    /// Converts a database document to MatchGroup
    /// </summary>
    public static MatchGroup ConvertDocumentToMatchGroup(IDictionary<string, object> doc)
    {
        var group = new MatchGroup
        {
            GroupId = doc.TryGetValue("GroupId", out var groupId)
                ? Convert.ToInt32(groupId)
                : 0,

            GroupHash = doc.TryGetValue("GroupHash", out var hash)
                ? hash?.ToString()
                : string.Empty,

            Records = doc.TryGetValue("Records", out var records)
                ? ConvertToRecordsList(records)
                : new List<IDictionary<string, object>>(),

            Metadata = doc.TryGetValue("Metadata", out var metadata)
                ? ConvertToDictionary(metadata)
                : new Dictionary<string, object>()
        };

        return group;
    }

    /// <summary>
    /// Converts MatchGroup to database document
    /// </summary>
    public static IDictionary<string, object> ConvertMatchGroupToDocument(MatchGroup group)
    {
        return new Dictionary<string, object>
        {
            ["GroupId"] = group.GroupId,
            ["GroupHash"] = group.GroupHash,
            ["Records"] = group.Records,
            ["Metadata"] = group.Metadata ?? new Dictionary<string, object>(),
            ["ProcessedAt"] = DateTime.UtcNow
        };
    }

    /// <summary>
    /// Converts object to list of dictionaries
    /// </summary>
    private static List<IDictionary<string, object>> ConvertToRecordsList(object records)
    {
        if (records is List<IDictionary<string, object>> recordsList)
            return recordsList;

        if (records is IEnumerable<object> enumerable)
        {
            return enumerable
                .Select(r => r as IDictionary<string, object> ?? new Dictionary<string, object>())
                .ToList();
        }

        return new List<IDictionary<string, object>>();
    }

    /// <summary>
    /// Converts object to dictionary
    /// </summary>
    private static Dictionary<string, object> ConvertToDictionary(object metadata)
    {
        if (metadata is Dictionary<string, object> dict)
            return dict;

        if (metadata is IDictionary<string, object> idict)
            return new Dictionary<string, object>(idict);

        return new Dictionary<string, object>();
    }
}