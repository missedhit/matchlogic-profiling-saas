namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class FieldOverwriteConfig
{
    public int BatchSize { get; set; } = 1000;
    public int ChannelCapacity { get; set; } = 100;
    public int MaxConcurrentBatches { get; set; } = 4;
    public bool EnableDetailedLogging { get; set; } = false;
    public bool ValidateRules { get; set; } = true;

    public static FieldOverwriteConfig Default()
    {
        return new FieldOverwriteConfig();
    }
}
