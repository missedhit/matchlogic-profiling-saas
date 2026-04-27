using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace MatchLogic.Domain.CleansingAndStandaradization
{
    public class ColumnValue
    {
        /// <summary>
        /// The name of the column
        /// </summary>
        public string Name { get; set; }
        public object Value { get; set; }

        /// <summary>
        /// Original data type of the value
        /// </summary>
        public Type OriginalType { get; set; }

        /// <summary>
        /// List of transformations that have been applied to this value
        /// </summary>
        public List<string> AppliedTransformations { get; } = new List<string>();

        /// <summary>
        /// Creates a new column value with the specified name and value
        /// </summary>
        public ColumnValue(string name, object value)
        {
            Name = name;
            Value = value;
            OriginalType = value?.GetType();
        }

        /// <summary>
        /// Creates a copy of this column value
        /// </summary>
        public ColumnValue Clone()
        {
            var clone = new ColumnValue(Name, Value)
            {
                OriginalType = OriginalType
            };
            clone.AppliedTransformations.AddRange(AppliedTransformations);
            return clone;
        }

        /// <summary>
        /// Tries to recover the original type after string transformations
        /// </summary>
        public void TryRecoverOriginalType()
        {
            if (OriginalType == null || Value == null || !(Value is string stringValue))
                return;

            if (OriginalType == typeof(DateTime))
            {
                if (DateTime.TryParse(stringValue, out DateTime value))
                {
                    Value = value;
                }
            }
            else if (OriginalType == typeof(Int64))
            {
                if (Int64.TryParse(stringValue, out Int64 value))
                {
                    Value = value;
                }
            }
            else if (OriginalType == typeof(Int32))
            {
                if (Int32.TryParse(stringValue, out Int32 value))
                {
                    Value = value;
                }
            }
            else if (OriginalType == typeof(Decimal))
            {
                if (Decimal.TryParse(stringValue, out Decimal value))
                {
                    Value = value;
                }
            }
            else if (OriginalType == typeof(Double))
            {
                if (Double.TryParse(stringValue, out Double value))
                {
                    Value = value;
                }
            }
            else if (OriginalType == typeof(Boolean))
            {
                if (Boolean.TryParse(stringValue, out Boolean value))
                {
                    Value = value;
                }
            }
        }

        /// <summary>
        /// Returns a string representation of this column value
        /// </summary>
        public override string ToString()
        {
            return $"{Name}: {Value}";
        }
    }
}
