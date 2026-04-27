using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Domain.CleansingAndStandaradization
{
    public class Record
    {
        /// <summary>
        /// Collection of column values in this record
        /// </summary>
        private readonly Dictionary<string, ColumnValue> _columns = new Dictionary<string, ColumnValue>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Gets a column value by name
        /// </summary>
        public ColumnValue this[string columnName]
        {
            get
            {
                if (_columns.TryGetValue(columnName, out var column))
                    return column;
                return null;
            }
        }

        /// <summary>
        /// Gets all column names in this record
        /// </summary>
        public IEnumerable<string> ColumnNames => _columns.Keys;

        /// <summary>
        /// Gets all column values in this record
        /// </summary>
        public IEnumerable<ColumnValue> Columns => _columns.Values;

        /// <summary>
        /// Gets the number of columns in this record
        /// </summary>
        public int ColumnCount => _columns.Count;

        /// <summary>
        /// Adds a column value to this record
        /// </summary>
        public void AddColumn(ColumnValue column)
        {
            _columns[column.Name] = column;
        }

        /// <summary>
        /// Adds a column value to this record
        /// </summary>
        public void AddColumn(string name, object value)
        {
            _columns[name] = new ColumnValue(name, value);
        }

        /// <summary>
        /// Checks if a column exists in this record
        /// </summary>
        public bool HasColumn(string columnName)
        {
            return _columns.ContainsKey(columnName);
        }

        /// <summary>
        /// Removes a column from this record
        /// </summary>
        public bool RemoveColumn(string columnName)
        {
            return _columns.Remove(columnName);
        }

        /// <summary>
        /// Creates a copy of this record with all column values
        /// </summary>
        public Record Clone()
        {
            var clone = new Record();
            foreach (var column in _columns.Values)
            {
                clone.AddColumn(column.Clone());
            }
            return clone;
        }

        /// <summary>
        /// Creates a record from a dictionary of values
        /// </summary>
        public static Record FromDictionary(IDictionary<string, object> values)
        {
            var record = new Record();
            foreach (var kvp in values)
            {
                if (kvp.Key != "_id")
                    record.AddColumn(kvp.Key, kvp.Value);
            }
            return record;
        }

        /// <summary>
        /// Converts this record to a dictionary
        /// </summary>
        public Dictionary<string, object> ToDictionary()
        {
            return _columns.Where(x => x.Key != "_id").ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value.Value,
                StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns the number of columns in this record
        /// </summary>
        public override string ToString()
        {
            return $"Record with {_columns.Count} columns";
        }
    }

    /// <summary>
    /// Represents a batch of records flowing through the transformation pipeline
    /// </summary>
    public class RecordBatch
    {
        /// <summary>
        /// List of records in this batch
        /// </summary>
        public List<Record> Records { get; } = new List<Record>();

        /// <summary>
        /// Gets the number of records in this batch
        /// </summary>
        public int Count => Records.Count;

        /// <summary>
        /// Gets a record by index
        /// </summary>
        public Record this[int index] => Records[index];

        /// <summary>
        /// Adds a record to this batch
        /// </summary>
        public void Add(Record record)
        {
            Records.Add(record);
        }

        /// <summary>
        /// Adds multiple records to this batch
        /// </summary>
        public void AddRange(IEnumerable<Record> records)
        {
            Records.AddRange(records);
        }

        /// <summary>
        /// Creates a shallow copy of this batch (references the same records)
        /// </summary>
        public RecordBatch ShallowCopy()
        {
            var copy = new RecordBatch();
            copy.Records.AddRange(Records);
            return copy;
        }

        /// <summary>
        /// Creates a deep copy of this batch (clones all records)
        /// </summary>
        public RecordBatch DeepCopy()
        {
            var copy = new RecordBatch();
            foreach (var record in Records)
            {
                copy.Add(record.Clone());
            }
            return copy;
        }

        /// <summary>
        /// Converts all records in this batch to dictionaries
        /// </summary>
        public List<Dictionary<string, object>> ToDictionaries()
        {
            return Records.Select(r => r.ToDictionary()).ToList();
        }

        /// <summary>
        /// Creates a batch from a list of dictionaries
        /// </summary>
        public static RecordBatch FromDictionaries(IEnumerable<IDictionary<string, object>> dictionaries)
        {
            var batch = new RecordBatch();
            foreach (var dictionary in dictionaries)
            {
                batch.Add(Record.FromDictionary(dictionary));
            }
            return batch;
        }

        /// <summary>
        /// Splits this batch into smaller batches of the specified size
        /// </summary>
        public IEnumerable<RecordBatch> Split(int batchSize)
        {
            for (int i = 0; i < Records.Count; i += batchSize)
            {
                var batch = new RecordBatch();
                batch.Records.AddRange(Records.Skip(i).Take(batchSize));
                yield return batch;
            }
        }

        /// <summary>
        /// Returns a string representation of this batch
        /// </summary>
        public override string ToString()
        {
            return $"Batch with {Records.Count} records";
        }
    }
}

