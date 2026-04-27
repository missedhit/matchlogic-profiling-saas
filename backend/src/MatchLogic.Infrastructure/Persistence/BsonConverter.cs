using LiteDB;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text;
using System.Threading.Tasks;

namespace MatchLogic.Infrastructure.Persistence;

public static class BsonConverter
{
    public static List<BsonDocument> ConvertBatch(IEnumerable<IDictionary<string, object>> batch)
    {
        return batch.Select(ConvertToBsonDocument).ToList();
    }

    public static BsonDocument ConvertToBsonDocument(IDictionary<string, object> dictionary)
    {
        var bsonDoc = new BsonDocument();
        foreach (var kvp in dictionary)
        {
            if (kvp.Key == "_id")
                continue;
            bsonDoc[kvp.Key] = ConvertToBsonValue(kvp.Value);
        }
        return bsonDoc;
    }

    /// <summary>
    /// Converts a System.Text.Json.JsonElement to its native .NET type.
    /// Required when ASP.NET Core deserializes JSON into IDictionar<string, object>
    /// </summary>
    public static object ConvertJsonElement(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                var str = element.GetString();

                if (!string.IsNullOrEmpty(str) && Guid.TryParse(str, out var guidVal))
                {
                    return guidVal;
                }
                // Try to parse as DateTime if it looks like an ISO date
                if (!string.IsNullOrEmpty(str) &&
                    (str.Contains("T") || (str.Contains("-") && str.Length > 8)) &&
                    DateTime.TryParse(str, out var dateTime))
                {
                    return dateTime;
                }
                return str;

            case JsonValueKind.Number:
                if (element.TryGetInt32(out var intVal))
                    return intVal;
                if (element.TryGetInt64(out var longVal))
                    return longVal;
                if (element.TryGetDouble(out var doubleVal))
                    return doubleVal;
                return element.GetDecimal();

            case JsonValueKind.True:
                return true;

            case JsonValueKind.False:
                return false;

            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                return null;

            case JsonValueKind.Array:
                var list = new List<object>();
                foreach (var item in element.EnumerateArray())
                {
                    list.Add(ConvertJsonElement(item));
                }
                return list;

            case JsonValueKind.Object:
                var dict = new Dictionary<string, object>();
                foreach (var prop in element.EnumerateObject())
                {
                    dict[prop.Name] = ConvertJsonElement(prop.Value);
                }
                return dict;

            default:
                return element.ToString();
        }
    }

    public static BsonValue ConvertToBsonValue(object value)
    {
        if (value == null)
            return BsonValue.Null;

        if (value is JsonElement jsonElement)
        {
            // Convert JsonElement to native type, then recursively convert to BsonValue
            var nativeValue = ConvertJsonElement(jsonElement);
            return ConvertToBsonValue(nativeValue);  // Recursive call with converted value
        }


        // Handle basic types that BsonValue supports directly
        if (value is string str) return new BsonValue(str);
        if (value is int intVal) return new BsonValue(intVal);
        if (value is long longVal) return new BsonValue(longVal);
        if (value is double doubleVal) return new BsonValue(doubleVal);
        if (value is decimal decimalVal) return new BsonValue(decimalVal);
        if (value is bool boolVal) return new BsonValue(boolVal);
        if (value is DateTime dateTimeVal) return new BsonValue(dateTimeVal);
        if (value is Guid guidVal) return new BsonValue(guidVal);
        if (value is byte[] bytes) return new BsonValue(bytes);

        // Handle dictionaries
        if (value is IDictionary<string, object> dict)
            return ConvertToBsonDocument(dict);

        // Handle any other dictionary with string key
        if (value.GetType().IsGenericType &&
            value.GetType().GetGenericTypeDefinition() == typeof(Dictionary<,>) &&
            value.GetType().GetGenericArguments()[0] == typeof(string))
        {
            dict = new Dictionary<string, object>();
            var enumerator = ((System.Collections.IEnumerable)value).GetEnumerator();
            while (enumerator.MoveNext())
            {
                var key = enumerator.Current.GetType().GetProperty("Key").GetValue(enumerator.Current) as string;
                var val = enumerator.Current.GetType().GetProperty("Value").GetValue(enumerator.Current);
                dict[key] = val;
            }
            return ConvertToBsonDocument(dict);
        }
        // Check if it's an array
        if (value is Array array)
        {
            // If it's a multidimensional array
            if (array.Rank > 1)
            {
                return ConvertMultidimensionalArrayToBsonValue(array);
            }

            // Single-dimensional array handling
            var bsonArray = new BsonArray();
            foreach (var item in array)
            {
                bsonArray.Add(ConvertToBsonValue(item));
            }
            return bsonArray;
        }

        // Handle enumerables (but not strings, which are also IEnumerable)
        if (value is IEnumerable enumerable && !(value is string))
            return new BsonArray(enumerable.Cast<object>().Select(ConvertToBsonValue));

        // Handle complex objects using reflection
        if (value.GetType().IsClass)
            return HandleCustomObject(value);

        // Default fallback
        return new BsonValue(value.ToString());
    }
    /// <summary>
    /// Helper method to recursively fill a multidimensional array
    /// </summary>
    private static int FillMultidimensionalArray(Array array, BsonArray data, int[] indices, int dimension, int dataIndex)
    {
        if (dimension == array.Rank)
        {
            // We've reached the element, set it from the data
            if (dataIndex < data.Count)
            {
                object value = ConvertBsonValueToObject(data[dataIndex]);

                // Ensure the value is of the correct type for the array
                Type elementType = array.GetType().GetElementType();
                if (value != null && !elementType.IsAssignableFrom(value.GetType()))
                {
                    // Try to convert the value to the correct type
                    try
                    {
                        value = Convert.ChangeType(value, elementType);
                    }
                    catch
                    {
                        // If conversion fails, use default value
                        value = elementType.IsValueType ? Activator.CreateInstance(elementType) : null;
                    }
                }

                array.SetValue(value, indices);
            }
            return dataIndex + 1;
        }

        for (int i = 0; i < array.GetLength(dimension); i++)
        {
            indices[dimension] = i;
            dataIndex = FillMultidimensionalArray(array, data, indices, dimension + 1, dataIndex);
        }

        return dataIndex;
    }
    /// <summary>
    /// Converts a BsonValue back to a .NET object
    /// </summary>
    public static object ConvertBsonValueToObject(BsonValue bsonValue)
    {
        // Check the type of BsonValue and convert it to the appropriate .NET type
        if (bsonValue.IsNull) return null;
        if (bsonValue.IsString) return bsonValue.AsString;
        if (bsonValue.IsInt32) return bsonValue.AsInt32;
        if (bsonValue.IsInt64) return bsonValue.AsInt64;
        if (bsonValue.IsDouble) return bsonValue.AsDouble;
        if (bsonValue.IsDecimal) return bsonValue.AsDecimal;
        if (bsonValue.IsBoolean) return bsonValue.AsBoolean;
        if (bsonValue.IsDateTime) return bsonValue.AsDateTime;
        if (bsonValue.IsObjectId) return bsonValue.AsObjectId;
        if (bsonValue.IsGuid) return bsonValue.AsGuid;

        // Handle multidimensional arrays
        if (bsonValue.IsDocument &&
            bsonValue.AsDocument.ContainsKey("_type") &&
            bsonValue.AsDocument["_type"].AsString == "multidimensional_array")
        {
            return ConvertBsonValueToMultidimensionalArray(bsonValue);
        }

        // Handle arrays by converting each element
        if (bsonValue.IsArray)
        {
            var array = bsonValue.AsArray;
            return array.Select(ConvertBsonValueToObject).ToList();
        }

        // Handle documents by converting to dictionary
        if (bsonValue.IsDocument)
        {
            return ConvertBsonDocumentToDictionary(bsonValue.AsDocument);
        }

        // Handle other types as needed
        return bsonValue.RawValue;
    }
    /// <summary>
    /// Converts a BsonDocument to a dictionary
    /// </summary>
    public static IDictionary<string, object> ConvertBsonDocumentToDictionary(BsonDocument doc)
    {
        return doc.ToDictionary(kvp => kvp.Key, kvp => ConvertBsonValueToObject(kvp.Value));
    }
    /// <summary>
    /// Converts a BsonValue containing a multidimensional array back to a .NET array
    /// </summary>
    public static object ConvertBsonValueToMultidimensionalArray(BsonValue bsonValue)
    {
        if (!bsonValue.IsDocument ||
            !bsonValue.AsDocument.ContainsKey("_type") ||
            bsonValue.AsDocument["_type"].AsString != "multidimensional_array")
        {
            return null;
        }

        var doc = bsonValue.AsDocument;
        int rank = doc["rank"].AsInt32;

        // Get dimensions
        var dimensionsArray = doc["dimensions"].AsArray;
        int[] dimensions = new int[rank];
        for (int i = 0; i < rank; i++)
        {
            dimensions[i] = dimensionsArray[i].AsInt32;
        }

        // Determine the type of array elements
        var dataArray = doc["data"].AsArray;
        Type elementType = typeof(object);

        // Try to determine element type from the first element
        if (dataArray.Count > 0)
        {
            var firstElement = dataArray[0];
            if (firstElement.IsInt32) elementType = typeof(int);
            else if (firstElement.IsInt64) elementType = typeof(long);
            else if (firstElement.IsDouble) elementType = typeof(double);
            else if (firstElement.IsDecimal) elementType = typeof(decimal);
            else if (firstElement.IsBoolean) elementType = typeof(bool);
            else if (firstElement.IsString) elementType = typeof(string);
            // Add more type checks as needed
        }

        // Create the array
        Array result = Array.CreateInstance(elementType, dimensions);

        // Fill the array
        FillMultidimensionalArray(result, dataArray, new int[rank], 0, 0);

        return result;
    }
    /// <summary>
    /// Converts a multidimensional array to a BsonValue with preserved structure
    /// </summary>
    public static BsonValue ConvertMultidimensionalArrayToBsonValue(Array array)
    {
        if (array == null)
            return BsonValue.Null;

        // Get the rank (number of dimensions)
        int rank = array.Rank;

        // For multidimensional arrays, we need to store both dimensions and data
        var dimensions = new int[rank];
        for (int i = 0; i < rank; i++)
        {
            dimensions[i] = array.GetLength(i);
        }

        // Create a flat array to store the data
        var flattenedData = new BsonArray();

        // Serialize the dimensions
        var dimensionsArray = new BsonArray();
        foreach (var dim in dimensions)
        {
            dimensionsArray.Add(new BsonValue(dim));
        }

        // Flatten the multidimensional array using recursion
        FlattenArray(array, new int[rank], 0, flattenedData);

        // Create a document to store both the dimensions and the data
        var result = new BsonDocument
        {
            ["_type"] = "multidimensional_array",
            ["rank"] = rank,
            ["dimensions"] = dimensionsArray,
            ["data"] = flattenedData
        };

        return result;
    }
    /// <summary>
    /// Helper method to recursively flatten multidimensional array
    /// </summary>
    private static void FlattenArray(Array array, int[] indices, int dimension, BsonArray result)
    {
        if (dimension == array.Rank)
        {
            // We've reached the element, add it to the result
            result.Add(ConvertToBsonValue(array.GetValue(indices)));
            return;
        }

        for (int i = 0; i < array.GetLength(dimension); i++)
        {
            indices[dimension] = i;
            FlattenArray(array, indices, dimension + 1, result);
        }
    }

    private static BsonDocument HandleCustomObject(object obj)
    {
        var bsonDoc = new BsonDocument();
        var properties = obj.GetType().GetProperties()
            .Where(p => p.CanRead);

        foreach (var prop in properties)
        {
            var value = prop.GetValue(obj);
            bsonDoc[prop.Name] = ConvertToBsonValue(value);
        }

        return bsonDoc;
    }

    public static BsonDocument SerializeToDocument<T>(T obj)
    {
        var document = new BsonDocument();
        if (obj == null) return document;

        var properties = typeof(T).GetProperties();

        foreach (var prop in properties)
        {
            var value = prop.GetValue(obj);
            if (value != null)
            {
                var bsonValue = ConvertToBsonValue(value);
                document[prop.Name] = bsonValue;
            }
        }

        return document;
    }

    public static void RegisterGuidDictionaryType<TValue>(BsonMapper mapper)
    {
        mapper.RegisterType<Dictionary<Guid, TValue>>(
            serialize: dict => {
                var doc = new BsonDocument();
                foreach (var pair in dict)
                    doc[pair.Key.ToString()] = mapper.Serialize(pair.Value);
                return doc;
            },
            deserialize: bson => {
                var dict = new Dictionary<Guid, TValue>();
                foreach (var pair in bson.AsDocument)
                    dict[Guid.Parse(pair.Key)] = mapper.Deserialize<TValue>(pair.Value);
                return dict;
            });
    }

    /// <summary>
    /// Register type mappings for multidimensional arrays
    /// </summary>
    public static void RegisterMultidimensionalArrayTypes(BsonMapper mapper)
    {
        // Register double[,] for correlation matrices
        mapper.RegisterType<double[,]>(
            // Serialize
            serialize: (array) => ConvertMultidimensionalArrayToBsonValue(array),
            // Deserialize
            deserialize: (bson) => (double[,])ConvertBsonValueToMultidimensionalArray(bson)
        );

        // Register int[,] for confusion matrices or similar
        mapper.RegisterType<int[,]>(
             // Serialize
             serialize: (array) => ConvertMultidimensionalArrayToBsonValue(array),
            // Deserialize
            deserialize: (bson) => (int[,])ConvertBsonValueToMultidimensionalArray(bson)
        );

        // Add more multidimensional array types as needed
    }

    /// <summary>
    /// Register type mappings for dictionaries with string keys and long values
    /// </summary>
    /// <param name="mapper"></param>
    public static void RegisterDictionaryTypes(BsonMapper mapper)
    {
        mapper.RegisterType(
           serialize: obj =>
           {
               var dict = (IDictionary<string, long>)obj;

               var array = new BsonArray();
               foreach (var kvp in dict)
               {
                   array.Add(new BsonDocument
                   {
                       ["Key"] = kvp.Key,
                       ["Value"] = kvp.Value
                   });
               }

               return array;
           },
           deserialize: bson =>
           {
               var array = bson.AsArray;
               var dict = new Dictionary<string, long>(StringComparer.Ordinal);

               foreach (var item in array)
               {
                   var doc = item.AsDocument;
                   var key = doc["Key"].AsString;
                   var value = doc["Value"].AsInt64;
                   dict[key] = value;
               }

               return dict;
           }
           );
    }
}

