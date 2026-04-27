using System.Collections.Generic;
using System;

public record BaseDictionaryCategoryDTO
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
    public List<string> Items { get; set; } = [];
    public bool IsSystem { get; set; }
    public bool IsDefault { get; set; }
    public int Version { get; set; }
}