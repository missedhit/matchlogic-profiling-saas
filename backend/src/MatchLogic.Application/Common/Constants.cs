using MatchLogic.Application.Features.DataMatching.FellegiSunter;
using MatchLogic.Domain.Analytics;
using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;

namespace MatchLogic.Application.Common;
public static class Constants
{
    public static class Collections
    {
        public const string MatchDefinition = "matchDefinition";
        public const string JobStatus = "jobStatus";
        public const string Projects = "projects";
        public const string ProjectRuns = "project_runs";
        public const string StepJobs = "step_jobs";
        public const string DataSources = "dataSources";
        public const string CleaningRules = "CleaningRules";
        public const string MergeRules = "MergeRules";
        public const string ExportSettings = "ExportSettings";
        public const string RegexInfo = "RegexInfo";
        public const string DictionaryCategory = "DictionaryCategory";
        public const string ImportFile = "FileImport";
        public const string MatchDefinitionCollection = "MatchDefinitionCollection";
        public const string MatchDataSourcePairs = "MatchDataSourcePairs";
        public const string MatchSettings = "MatchSettings";
        // List of fields in project 
        public const string FieldMapping = "FieldMapping";
        // MappedRows in project
        public const string MappedFieldRows = "MappedFieldRows";
        public const string WordSmithDictionary = "WordSmithDictionary";
        public const string WordSmithDictionaryRules = "WordSmithDictionaryRules";
        public const string TransformationGraphs = "TransformationGraphs";
        public const string ProperCaseOptions = "ProperCaseOptions";
        public const string DataSourceColumnNotes = "DataSourceColumnNotes";

        public const string FinalExportSettings = "FinalExportSettings";
        public const string FinalExportResults = "FinalExportResults";
        public const string DataSnapshots = "DataSnapshots";
        public const string MasterRecordRuleSets = "MasterRecordRuleSets";
        public const string FieldOverwriteRuleSets = "FieldOverwriteRuleSets";
        public const string ScoreBand = "ScoreBand";

        public const string ScheduledTasks = "ScheduledTasks";
        public const string ScheduledTaskExecutions = "ScheduledTaskExecutions";
    }

    public static List<ScoreBandDME> bands = new List<ScoreBandDME>
        {
            new ScoreBandDME { Label = "Excellent", MinThreshold = 0.90, MaxThreshold = 1.01 },
            new ScoreBandDME { Label = "High", MinThreshold = 0.80, MaxThreshold = 0.90 },
            new ScoreBandDME { Label = "Good", MinThreshold = 0.70, MaxThreshold = 0.80 },
            new ScoreBandDME { Label = "Moderate", MinThreshold = 0.60, MaxThreshold = 0.70 },
            new ScoreBandDME { Label = "Low", MinThreshold = 0.50, MaxThreshold = 0.60 },
            new ScoreBandDME { Label = "Poor", MinThreshold = 0.0, MaxThreshold = 0.50 }
        };
    public static class DefaultOptions
    {
        public static ProbabilisticOption probabilisticOption = new ProbabilisticOption() { DecimalPlaces = 6 };
    }

    public static class Scheduler
    {
        public static class DefaultValues
        {
            public const int MaxRetryAttempts = 3;
            public const int MaxConsecutiveFailuresBeforeSuspend = 5;
        }
    }
    public static class FieldNames
    {
        public const string DataSourceId = "DataSourceId";
        public const string ExportId = "ExportId";
    }

    public static class FieldLength
    {
        public static int NameMaxLength = 150;
        public static int DescriptionMaxLength = 2000;

    }

    public static class GenderConstants
    {
        // Gender constants
        public static string GenderUndefined = "?";
        public static string GenderMale = "M";
        public static string GenderFemale = "F";
        public static string GenderMaleAndFemale = "M & F";
        public static string GenderFemaleAndMale = "F & M";
    }
}
public enum StoreType
{
    LiteDb,
    InMemory,
    ProgressLiteDb,
    MongoDB = 4,
    ProgressMongoDB = 5
}

[AttributeUsage(AttributeTargets.Class)]
public class UseStoreAttribute : Attribute
{
    public StoreType StoreType { get; }

    public UseStoreAttribute(StoreType storeType)
    {
        StoreType = storeType;
    }
}
[AttributeUsageAttribute(AttributeTargets.Class)]
public class CleansingOperationAttribute : Attribute
{
    public CleaningRuleType CleaningRuleType { get; }
    public CleansingOperationAttribute(CleaningRuleType cleaningRuleType)
    {
        CleaningRuleType = cleaningRuleType;
    }
}