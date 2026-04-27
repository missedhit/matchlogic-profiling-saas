using MatchLogic.Application.Extensions;
using System;

namespace MatchLogic.Application.Common;

public static class DatasetNames
{
    public static string SnapshotPrefix(Guid snapshotId)
        => $"snap_{GuidCollectionNameConverter.ToValidCollectionName(snapshotId)}";

    public static string SnapshotRows(Guid snapshotId)
        => $"{SnapshotPrefix(snapshotId)}_rows";
}
