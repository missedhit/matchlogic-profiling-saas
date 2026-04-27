using System;
using System.Collections.Generic;

namespace MatchLogic.Domain.MergeAndSurvivorship
{
    /// <summary>
    /// Result of field overwriting operation
    /// </summary>
    public class OverwriteResult
    {
        public Guid ProjectId { get; set; }
        public bool Success { get; set; }
        public string ErrorMessage { get; set; }
        public int TotalGroupsProcessed { get; set; }
        public int TotalFieldsOverwritten { get; set; }
        public Dictionary<string, int> FieldOverwriteCounts { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public TimeSpan Duration => EndTime - StartTime;
        public OverwriteOutputCollections OutputCollections { get; set; }

        public OverwriteResult()
        {
            FieldOverwriteCounts = new Dictionary<string, int>();
            OutputCollections = new OverwriteOutputCollections();
        }
    }

    /// <summary>
    /// Output collection names from field overwriting operation
    /// </summary>
    public class OverwriteOutputCollections
    {
        public string InputGroupsCollection { get; set; }
        public string OverwrittenGroupsCollection { get; set; }
        public string AuditLogCollection { get; set; }
    }
}
