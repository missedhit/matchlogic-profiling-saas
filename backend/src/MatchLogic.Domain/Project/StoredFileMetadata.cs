namespace MatchLogic.Domain.Project;

/// <summary>
/// Metadata captured from a remote file at the time of import.
/// Used to detect whether the remote file has changed since last import.
/// </summary>
public class StoredFileMetadata
{
    public string? LastModified { get; set; }
    public string? ETag { get; set; }
    public long Size { get; set; }
    public string? ContentType { get; set; }
}
