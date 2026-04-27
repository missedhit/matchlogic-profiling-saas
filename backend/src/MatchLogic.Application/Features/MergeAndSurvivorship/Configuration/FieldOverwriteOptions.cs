namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class FieldOverwriteOptions
{
    public bool CreateBackup { get; set; }
    public bool ValidateRuleSet { get; set; }

    public static FieldOverwriteOptions Default()
    {
        return new FieldOverwriteOptions
        {
            CreateBackup = false,
            ValidateRuleSet = true
        };
    }
}
