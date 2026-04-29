namespace MatchLogic.Infrastructure.Storage;

public class S3Options
{
    public const string SectionName = "S3";

    public string BucketName { get; set; } = string.Empty;

    public string Region { get; set; } = "us-east-1";

    public int PresignedUploadExpiryMinutes { get; set; } = 5;

    public string UploadKeyPrefix { get; set; } = "uploads/";
}
