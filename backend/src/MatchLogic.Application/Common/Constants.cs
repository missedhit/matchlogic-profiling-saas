using MatchLogic.Domain.Entities.Common;
using System;

namespace MatchLogic.Application.Common;

public static class Constants
{
    public static class Collections
    {
        public const string JobStatus = "jobStatus";
        public const string Projects = "projects";
        public const string ProjectRuns = "project_runs";
        public const string StepJobs = "step_jobs";
        public const string DataSources = "dataSources";
        public const string RegexInfo = "RegexInfo";
        public const string DictionaryCategory = "DictionaryCategory";
        public const string ImportFile = "FileImport";
        public const string FieldMapping = "FieldMapping";
        public const string DataSourceColumnNotes = "DataSourceColumnNotes";
        public const string DataSnapshots = "DataSnapshots";
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
