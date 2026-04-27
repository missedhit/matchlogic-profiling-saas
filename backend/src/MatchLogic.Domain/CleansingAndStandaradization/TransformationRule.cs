using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization;

/// <summary>
/// Abstract base class for all transformation rules
/// </summary>
public abstract class TransformationRule
{
    /// <summary>
    /// Unique identifier for this rule
    /// </summary>
    public Guid Id { get; } = Guid.NewGuid();

    /// <summary>
    /// Gets the columns affected by this transformation rule
    /// </summary>
    public abstract IEnumerable<string> AffectedColumns { get; }
    public abstract IEnumerable<string> OutputColumns { get; }
    /// <summary>
    /// Gets the IDs of rules that this rule depends on
    /// </summary>
    public abstract IEnumerable<Guid> DependsOn { get; }

    /// <summary>
    /// Gets the priority of this rule (lower numbers execute first)
    /// </summary>
    public virtual int Priority { get; } = 100;

    /// <summary>
    /// Applies this transformation rule to a record
    /// </summary>
    public abstract void Apply(Record record);

    /// <summary>
    /// Determines if this rule can be applied to the given record
    /// </summary>
    public virtual bool CanApply(Record record)
    {
        foreach (var column in AffectedColumns)
        {
            if (!record.HasColumn(column))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Returns a string representation of this rule
    /// </summary>
    public override string ToString()
    {
        return $"{GetType().Name} (ID: {Id})";
    }
}
/// <summary>
/// Enhanced transformation rule with input/output column tracking
/// </summary>
public abstract class EnhancedTransformationRule : TransformationRule
{
    /// <summary>
    /// Columns that this rule reads from (inputs)
    /// </summary>
    public abstract IEnumerable<string> InputColumns { get; }

    /// <summary>
    /// Columns that this rule writes to (outputs)
    /// </summary>
    //public new abstract IEnumerable<string> OutputColumns { get; }

    /// <summary>
    /// Columns that this rule modifies in-place
    /// </summary>
    public virtual IEnumerable<string> ModifiedColumns => Enumerable.Empty<string>();

    /// <summary>
    /// Whether this rule creates new columns
    /// </summary>
    public virtual bool CreatesNewColumns => OutputColumns.Any(col => !InputColumns.Contains(col));
    public new virtual int Priority { get; set; } = 100;
}