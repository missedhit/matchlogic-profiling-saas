using System;
using System.Collections.Generic;

namespace MatchLogic.Application.Features.MergeAndSurvivorship;

public class FieldOverwriteResult
{
    public Guid ProjectId { get; set; }
    public DateTime StartTime { get; set; }
    public DateTime EndTime { get; set; }
    public bool Success { get; set; }
    public string ErrorMessage { get; set; }
    public FieldOverwriteCollectionNames OutputCollections { get; set; }
    public int TotalGroupsProcessed { get; set; }
    public int TotalFieldsOverwritten { get; set; }
    
    public TimeSpan Duration => EndTime - StartTime;
}
