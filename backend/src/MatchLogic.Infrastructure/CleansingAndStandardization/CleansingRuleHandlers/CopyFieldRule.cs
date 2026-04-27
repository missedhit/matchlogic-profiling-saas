using MatchLogic.Domain.CleansingAndStandaradization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.CleansingAndStandardization.CleansingRuleHandlers;

/// <summary>
/// Rule for copying a field to a new column
/// </summary>
public class CopyFieldRule : TransformationRule
{
    private readonly string _sourceColumn;
    private readonly string _targetColumn;
    private readonly IEnumerable<Guid> _dependencies;

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public override IEnumerable<string> AffectedColumns => new[] { _sourceColumn };

    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public override IEnumerable<Guid> DependsOn => _dependencies;

    /// <summary>
    /// Gets the priority of this rule (copy should happen early)
    /// </summary>
    public override int Priority => 1;

    public override IEnumerable<string> OutputColumns => new[] { _targetColumn };
    /// <summary>
    /// Creates a new copy field rule
    /// </summary>
    public CopyFieldRule(
        string sourceColumn,
        string targetColumn,
        IEnumerable<Guid> dependencies = null)
    {
        _sourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
        _targetColumn = targetColumn ?? throw new ArgumentNullException(nameof(targetColumn));
        _dependencies = dependencies ?? Enumerable.Empty<Guid>();
    }

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public override void Apply(Record record)
    {
        if (record == null || !record.HasColumn(_sourceColumn))
            return;

        var sourceColumn = record[_sourceColumn];
        if (sourceColumn == null)
            return;

        // Create a copy of the source column with the new name
        var newColumn = new ColumnValue(_targetColumn, sourceColumn.Value)
        {
            OriginalType = sourceColumn.OriginalType
        };

        // Copy applied transformations
        newColumn.AppliedTransformations.AddRange(sourceColumn.AppliedTransformations);
        newColumn.AppliedTransformations.Add("Copy");

        // Add or replace the target column
        record.AddColumn(newColumn);
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"CopyFieldRule: {_sourceColumn} -> {_targetColumn} (ID: {Id})";
    }
}
