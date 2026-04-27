
using MatchLogic.Domain.Entities.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization;
public class WordSmithDictionary : AuditableEntity
{
    public string Name { get; set; }
    public string Description { get; set; }
    public string Category { get; set; }
    public string OriginalFileName { get; set; }
    public string OriginalFilePath { get; set; }
    public int Version { get; set; } = 1;
    public bool IsActive { get; set; } = true;

    public bool AddFlagColumn { get; set; }
    public string WordDelimiters { get; set; }

    public string MaxWords { get; set; }
}

/// <summary>
/// Individual dictionary rule stored in database
/// </summary>
public class WordSmithDictionaryRule : IEntity
{
    public Guid DictionaryId { get; set; }
    public string Words { get; set; }
    public string Replacement { get; set; }
    public string NewColumnName { get; set; }
    public bool ToDelete { get; set; }
    public int Count { get; set; }
    public int Priority { get; set; } = 5;
    public bool IsActive { get; set; } = true;
    public DateTime CreatedAt { get; set; }
    public DateTime? ModifiedAt { get; set; }
    public string ModifiedBy { get; set; }
}