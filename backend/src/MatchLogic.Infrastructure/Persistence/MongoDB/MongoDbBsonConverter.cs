using MatchLogic.Domain.Entities.Common;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.Serialization.IdGenerators;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using MatchLogic.Domain.DataProfiling;
using System.Text.Json;

namespace MatchLogic.Infrastructure.Persistence.MongoDB;

/// <summary>
/// Handles conversion between .NET objects and MongoDB BSON documents
/// Optimized for high-throughput scenarios
/// Equivalent to LiteDB's BsonConverter + BsonMapper registrations
/// </summary>
public static class MongoDbBsonConverter
{
    private static bool _conventionsRegistered = false;
    private static readonly object _lockObject = new object();

    /// <summary>
    /// Register MongoDB conventions once at startup - equivalent to LiteDB's BsonMapper.Global
    /// Call this ONCE at application startup before any MongoDB operations
    /// </summary>
    public static void RegisterConventions()
    {
        if (_conventionsRegistered) return;

        lock (_lockObject)
        {
            if (_conventionsRegistered) return;

            // 1. Register conventions (like LiteDB's global settings)
            var conventionPack = new ConventionPack
            {
                new IgnoreExtraElementsConvention(true),
                //new CamelCaseElementNameConvention(),
                new EnumRepresentationConvention(BsonType.Int32), 
                new IgnoreIfNullConvention(true)
            };
            ConventionRegistry.Register("MatchLogicConventions", conventionPack, _ => true);

            // 2. Register Guid serializer with Standard representation
            try
            {
                BsonSerializer.RegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));
            }
            catch { /* Already registered */ }

            // 3. Register Nullable<Guid> serializer
            try
            {
                BsonSerializer.RegisterSerializer(typeof(Guid?),
                    new NullableSerializer<Guid>(new GuidSerializer(GuidRepresentation.Standard)));
            }
            catch { /* Already registered */ }

            // 4. Register custom Dictionary<string, object> serializer
            try
            {
                BsonSerializer.RegisterSerializer(typeof(Dictionary<string, object>),
                    new DictionaryWithGuidSerializer());
            }
            catch { /* Already registered */ }

            // 5. Register IDictionary<string, object> as well
            try
            {
                BsonSerializer.RegisterSerializer(typeof(IDictionary<string, object>),
                    new IDictionaryWithGuidSerializer());
            }
            catch { /* Already registered */ }

            // 6. Register Universal Provider - handles ALL Dictionary<Enum, T> and ConcurrentDictionary automatically
            try
            {
                BsonSerializer.RegisterSerializationProvider(new UniversalSerializationProvider());
            }
            catch { /* Already registered */ }

            

            // ============================================================================
            // NOW register entities AFTER serializers are configured
            // ============================================================================
            RegisterEntityHierarchyAutomatically();

            if (!BsonClassMap.IsClassMapRegistered(typeof(ProfileResult)))
            {
                BsonClassMap.RegisterClassMap<ProfileResult>(cm =>
                {
                    cm.AutoMap();
                    cm.SetIgnoreExtraElements(true);
                    cm.SetIsRootClass(false); // Inherits from IEntity

                    // Custom serializer for ColumnProfiles
                    cm.MapProperty(c => c.ColumnProfiles)
                      .SetSerializer(new ConcurrentDictionarySerializer<string, ColumnProfile>());

                    System.Diagnostics.Debug.WriteLine("Registered ProfileResult class map with custom serializer");
                });
            }

            // Then manually register AdvancedProfileResult with virtual property handling
            if (!BsonClassMap.IsClassMapRegistered(typeof(AdvancedProfileResult)))
            {
                BsonClassMap.RegisterClassMap<AdvancedProfileResult>(cm =>
                {
                    cm.AutoMap();
                    cm.SetIgnoreExtraElements(true);
                    cm.SetIsRootClass(false); // Inherits from ProfileResult

                    // Unmap the helper property (domain stays clean)
                    cm.UnmapProperty("AdvancedColumnProfiles");

                    System.Diagnostics.Debug.WriteLine("Registered AdvancedProfileResult with virtual property mapping");
                });
            }

            _conventionsRegistered = true;
        }
    }

    /// <summary>
    /// Automatically discovers and registers all classes inheriting from Entity<Guid>
    /// This eliminates the need to manually register each new domain class
    /// </summary>
    private static void RegisterEntityHierarchyAutomatically()
    {
        try
        {
            // Get all loaded assemblies from the MatchLogic domain
            var assemblies = AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => a.FullName != null &&
                       (a.FullName.Contains("MatchLogic.Domain") ||
                        a.FullName.Contains("MatchLogic.Application")))
                .ToList();

            // Also try to load the Domain assembly explicitly if not loaded
            try
            {
                var domainAssembly = Assembly.Load("MatchLogic.Domain");
                if (!assemblies.Contains(domainAssembly))
                    assemblies.Add(domainAssembly);
            }
            catch { /* Assembly not found or already loaded */ }

            var typesToExcludeFromAutoRegistration = new HashSet<Type>
        {
            typeof(ProfileResult),
            typeof(AdvancedProfileResult)
        };

            // Find all types that inherit from Entity<Guid> or IEntity
            var entityTypes = assemblies
                .SelectMany(a =>
                {
                    try
                    {
                        return a.GetTypes();
                    }
                    catch (ReflectionTypeLoadException ex)
                    {
                        // Return only the types that loaded successfully
                        return ex.Types.Where(t => t != null);
                    }
                })
                .Where(t => t != null &&
                   !t.IsAbstract &&
                   !t.IsInterface &&
                   (IsEntityType(t) || IsIEntityType(t)) &&
                   !typesToExcludeFromAutoRegistration.Contains(t))
                .OrderBy(t => GetInheritanceDepth(t)) // Register base classes first
                .ToList();

            // Register base Entity<Guid> class first
            if (!BsonClassMap.IsClassMapRegistered(typeof(Entity<Guid>)))
            {
                BsonClassMap.RegisterClassMap<Entity<Guid>>(cm =>
                {
                    cm.AutoMap();
                    cm.MapIdProperty(c => c.Id)
                        .SetSerializer(new GuidSerializer(GuidRepresentation.Standard))
                        .SetIdGenerator(GuidGenerator.Instance);
                    cm.SetIgnoreExtraElements(true);
                });
            }

            // Register IEntity if it exists
            var iEntityType = assemblies
                .SelectMany(a =>
                {
                    try
                    {
                        return a.GetTypes();
                    }
                    catch (ReflectionTypeLoadException ex)
                    {
                        return ex.Types.Where(t => t != null);
                    }
                })
                .FirstOrDefault(t => t != null && t.Name == "IEntity" && t.Namespace == "MatchLogic.Domain.Entities.Common");

            if (iEntityType != null && !BsonClassMap.IsClassMapRegistered(iEntityType))
            {
                var cm = new BsonClassMap(iEntityType);
                cm.AutoMap();
                cm.SetIgnoreExtraElements(true);
                cm.SetIsRootClass(false);
                BsonClassMap.RegisterClassMap(cm);
            }

            // Register all entity types
            int registeredCount = 0;
            foreach (var entityType in entityTypes)
            {
                if (!BsonClassMap.IsClassMapRegistered(entityType))
                {
                    try
                    {
                        var classMap = new BsonClassMap(entityType);
                        classMap.AutoMap();
                        classMap.SetIgnoreExtraElements(true);
                        classMap.SetIsRootClass(false); // Not a root class - inherits from Entity<Guid>

                        // CRITICAL: Unmap properties that are hidden with 'new' keyword
                        UnmapHiddenProperties(classMap, entityType);

                        BsonClassMap.RegisterClassMap(classMap);
                        registeredCount++;
                    }
                    catch (Exception ex)
                    {
                        // Log warning but continue - some types might have issues
                        System.Diagnostics.Debug.WriteLine(
                            $"Warning: Could not register BsonClassMap for {entityType.FullName}: {ex.Message}");
                    }
                }
            }

            System.Diagnostics.Debug.WriteLine(
                $"MongoDB: Auto-registered {registeredCount} entity classes from {assemblies.Count} assemblies");
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine(
                $"Warning: Error during automatic entity registration: {ex.Message}");
            // Don't throw - let the application continue with default serialization
        }
    }

    /// <summary>
    /// Unmap properties that are hidden in derived classes using 'new' keyword
    /// This prevents MongoDB from trying to map both the base and derived property
    /// </summary>
    private static void UnmapHiddenProperties(BsonClassMap classMap, Type entityType)
    {
        // Get all properties declared in the current type with 'new' modifier
        var currentTypeProperties = entityType.GetProperties(
            BindingFlags.Public |
            BindingFlags.Instance |
            BindingFlags.DeclaredOnly);

        foreach (var property in currentTypeProperties)
        {
            // Check if this property hides a base class property
            var baseType = entityType.BaseType;
            while (baseType != null && baseType != typeof(object))
            {
                var baseProperty = baseType.GetProperty(
                    property.Name,
                    BindingFlags.Public | BindingFlags.Instance);

                if (baseProperty != null)
                {
                    // This property hides a base property
                    // Unmap the base property to avoid conflicts
                    try
                    {
                        var baseMemberMap = classMap.GetMemberMap(baseProperty.Name);
                        if (baseMemberMap != null)
                        {
                            classMap.UnmapProperty(baseProperty.Name);
                            System.Diagnostics.Debug.WriteLine(
                                $"Unmapped hidden property '{baseProperty.Name}' from base class in {entityType.Name}");
                        }
                    }
                    catch
                    {
                        // Property might not be mapped yet, ignore
                    }
                    break;
                }
                baseType = baseType.BaseType;
            }
        }
    }

    /// <summary>
    /// Check if type inherits from Entity<Guid>
    /// </summary>
    private static bool IsEntityType(Type type)
    {
        var baseType = type.BaseType;
        while (baseType != null)
        {
            if (baseType.IsGenericType &&
                baseType.GetGenericTypeDefinition() == typeof(Entity<>) &&
                baseType.GetGenericArguments()[0] == typeof(Guid))
            {
                return true;
            }
            baseType = baseType.BaseType;
        }
        return false;
    }

    /// <summary>
    /// Check if type inherits from IEntity
    /// </summary>
    private static bool IsIEntityType(Type type)
    {
        var baseType = type.BaseType;
        while (baseType != null)
        {
            if (baseType.Name == "IEntity" &&
                baseType.Namespace == "MatchLogic.Domain.Entities.Common")
            {
                return true;
            }
            baseType = baseType.BaseType;
        }
        return false;
    }

    /// <summary>
    /// Get inheritance depth to ensure base classes are registered first
    /// </summary>
    private static int GetInheritanceDepth(Type type)
    {
        int depth = 0;
        var baseType = type.BaseType;
        while (baseType != null && baseType != typeof(object))
        {
            depth++;
            baseType = baseType.BaseType;
        }
        return depth;
    }
    #region Batch Conversion Methods

    /// <summary>
    /// Converts a batch of dictionaries to BsonDocuments for bulk insert
    /// </summary>
    public static List<BsonDocument> ConvertBatch(IEnumerable<IDictionary<string, object>> batch)
    {
        return batch.Select(ConvertToBsonDocument).ToList();
    }

    #endregion

    #region Dictionary to BsonDocument Conversion

    /// <summary>
    /// Converts a dictionary to a BsonDocument
    /// </summary>
    public static BsonDocument ConvertToBsonDocument(IDictionary<string, object> dictionary)
    {
        var bsonDoc = new BsonDocument();

        foreach (var kvp in dictionary)
        {
            // Skip _id if present - let MongoDB generate it
            if (kvp.Key == "_id")
                continue;

            bsonDoc[kvp.Key] = ConvertToBsonValue(kvp.Value);
        }

        return bsonDoc;
    }

    /// <summary>
    /// Converts a System.Text.Json.JsonElement to its native .NET type.
    /// Required when ASP.NET Core deserializes JSON into IDictionary<string, object>
    /// </summary>
    private static object ConvertJsonElement(JsonElement element)
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

    /// <summary>
    /// Converts a .NET object to BsonValue
    /// </summary>
    public static BsonValue ConvertToBsonValue(object value)
    {
        if (value == null)
            return BsonNull.Value;

        // CRITICAL: Handle JsonElement FIRST (from ASP.NET Core deserialization)
        if (value is JsonElement jsonElement)
        {
            var nativeValue = ConvertJsonElement(jsonElement);
            return ConvertToBsonValue(nativeValue);  // Recursive call with converted value
        }

        if (value.GetType().IsEnum)
        {
            // Convert enum to its underlying integer value
            return new BsonInt32(Convert.ToInt32(value));
        }

        // Handle basic types
        return value switch
        {
            string str => new BsonString(str),
            int intVal => new BsonInt32(intVal),
            long longVal => new BsonInt64(longVal),
            double doubleVal => new BsonDouble(doubleVal),
            decimal decimalVal => new BsonDecimal128(decimalVal),
            float floatVal => new BsonDouble(floatVal),
            bool boolVal => new BsonBoolean(boolVal),
            DateTime dateTimeVal => new BsonDateTime(dateTimeVal),
            DateTimeOffset dtoVal => new BsonDateTime(dtoVal.UtcDateTime),
            Guid guidVal => new BsonBinaryData(guidVal, GuidRepresentation.Standard),
            byte[] bytes => new BsonBinaryData(bytes),
            ObjectId oid => oid,
            BsonValue bsonVal => bsonVal,

            // Handle dictionaries
            IDictionary<string, object> dict => ConvertToBsonDocument(dict),

            // Handle arrays
            Array array => ConvertArrayToBsonValue(array),

            // Handle other enumerables (but not strings)
            IEnumerable enumerable => ConvertEnumerableToBsonArray(enumerable),

            // Handle complex objects
            _ when value.GetType().IsClass => HandleComplexObject(value),

            // Default fallback
            _ => new BsonString(value.ToString())
        };
    }

    #endregion

    #region Array Conversion

    /// <summary>
    /// Converts an array to BsonValue, handling multidimensional arrays
    /// </summary>
    private static BsonValue ConvertArrayToBsonValue(Array array)
    {
        if (array.Rank > 1)
        {
            return ConvertMultidimensionalArrayToBsonValue(array);
        }

        var bsonArray = new BsonArray();
        foreach (var item in array)
        {
            bsonArray.Add(ConvertToBsonValue(item));
        }
        return bsonArray;
    }

    /// <summary>
    /// Converts an enumerable to BsonArray
    /// </summary>
    private static BsonArray ConvertEnumerableToBsonArray(IEnumerable enumerable)
    {
        var bsonArray = new BsonArray();
        foreach (var item in enumerable)
        {
            bsonArray.Add(ConvertToBsonValue(item));
        }
        return bsonArray;
    }

    /// <summary>
    /// Converts a multidimensional array to BsonDocument with preserved structure
    /// Equivalent to LiteDB's RegisterMultidimensionalArrayTypes
    /// </summary>
    public static BsonDocument ConvertMultidimensionalArrayToBsonValue(Array array)
    {
        if (array == null)
            return null;

        int rank = array.Rank;
        var dimensions = new int[rank];

        for (int i = 0; i < rank; i++)
        {
            dimensions[i] = array.GetLength(i);
        }

        var dimensionsArray = new BsonArray(dimensions.Select(d => new BsonInt32(d)));
        var flattenedData = new BsonArray();

        FlattenArray(array, new int[rank], 0, flattenedData);

        return new BsonDocument
        {
            ["_type"] = "multidimensional_array",
            ["rank"] = rank,
            ["dimensions"] = dimensionsArray,
            ["data"] = flattenedData
        };
    }

    /// <summary>
    /// Recursively flattens a multidimensional array
    /// </summary>
    private static void FlattenArray(Array array, int[] indices, int dimension, BsonArray result)
    {
        if (dimension == array.Rank)
        {
            result.Add(ConvertToBsonValue(array.GetValue(indices)));
            return;
        }

        for (int i = 0; i < array.GetLength(dimension); i++)
        {
            indices[dimension] = i;
            FlattenArray(array, indices, dimension + 1, result);
        }
    }

    #endregion

    #region Complex Object Handling

    /// <summary>
    /// Handles complex objects using reflection
    /// </summary>
    private static BsonDocument HandleComplexObject(object obj)
    {
        var bsonDoc = new BsonDocument();
        var properties = obj.GetType().GetProperties()
            .Where(p => p.CanRead);

        foreach (var prop in properties)
        {
            try
            {
                var value = prop.GetValue(obj);
                if (value != null)
                {
                    bsonDoc[prop.Name] = ConvertToBsonValue(value);
                }
            }
            catch
            {
                // Skip properties that throw exceptions
            }
        }

        return bsonDoc;
    }

    #endregion

    #region BsonDocument to Dictionary Conversion

    /// <summary>
    /// Converts a BsonDocument back to a dictionary
    /// </summary>
    public static IDictionary<string, object> ConvertBsonDocumentToDictionary(BsonDocument doc)
    {
        var result = new Dictionary<string, object>(StringComparer.Ordinal);

        foreach (var element in doc)
        {
            result[element.Name] = ConvertBsonValueToObject(element.Value);
        }

        return result;
    }

    /// <summary>
    /// Converts a BsonValue to a .NET object
    /// </summary>
    public static object ConvertBsonValueToObject(BsonValue bsonValue)
    {
        if (bsonValue == null || bsonValue.IsBsonNull)
            return null;

        return bsonValue.BsonType switch
        {
            BsonType.String => bsonValue.AsString,
            BsonType.Int32 => bsonValue.AsInt32,
            BsonType.Int64 => bsonValue.AsInt64,
            BsonType.Double => bsonValue.AsDouble,
            BsonType.Decimal128 => Decimal128.ToDecimal(bsonValue.AsDecimal128),
            BsonType.Boolean => bsonValue.AsBoolean,
            BsonType.DateTime => bsonValue.ToUniversalTime(),
            BsonType.ObjectId => bsonValue.AsObjectId,
            BsonType.Binary when IsGuidBinary(bsonValue.AsBsonBinaryData) => bsonValue.AsGuid,
            BsonType.Binary => bsonValue.AsByteArray,
            BsonType.Array => ConvertBsonArrayToList(bsonValue.AsBsonArray),
            BsonType.Document => ConvertBsonDocumentToObjectOrArray(bsonValue.AsBsonDocument),
            _ => BsonTypeMapper.MapToDotNetValue(bsonValue)
        };
    }

    /// <summary>
    /// Checks if BsonBinaryData represents a Guid
    /// </summary>
    private static bool IsGuidBinary(BsonBinaryData binaryData)
    {
        return binaryData.SubType == BsonBinarySubType.UuidStandard ||
               binaryData.SubType == BsonBinarySubType.UuidLegacy;
    }

    /// <summary>
    /// Converts BsonArray to List
    /// </summary>
    private static List<object> ConvertBsonArrayToList(BsonArray array)
    {
        return array.Select(ConvertBsonValueToObject).ToList();
    }

    /// <summary>
    /// Handles conversion of BsonDocument, including multidimensional array detection
    /// </summary>
    private static object ConvertBsonDocumentToObjectOrArray(BsonDocument doc)
    {
        // Check for multidimensional array
        if (doc.Contains("_type") && doc["_type"].AsString == "multidimensional_array")
        {
            return ConvertBsonValueToMultidimensionalArray(doc);
        }

        return ConvertBsonDocumentToDictionary(doc);
    }

    #endregion

    #region Multidimensional Array Deserialization

    /// <summary>
    /// Converts a BsonDocument containing a multidimensional array back to .NET array
    /// </summary>
    public static object ConvertBsonValueToMultidimensionalArray(BsonDocument doc)
    {
        int rank = doc["rank"].AsInt32;
        var dimensionsArray = doc["dimensions"].AsBsonArray;
        var dimensions = new int[rank];

        for (int i = 0; i < rank; i++)
        {
            dimensions[i] = dimensionsArray[i].AsInt32;
        }

        var dataArray = doc["data"].AsBsonArray;
        Type elementType = DetermineElementType(dataArray);

        Array result = Array.CreateInstance(elementType, dimensions);
        FillMultidimensionalArray(result, dataArray, new int[rank], 0, 0);

        return result;
    }

    /// <summary>
    /// Determines the element type from a BsonArray
    /// </summary>
    private static Type DetermineElementType(BsonArray dataArray)
    {
        if (dataArray.Count == 0)
            return typeof(object);

        var firstElement = dataArray[0];

        return firstElement.BsonType switch
        {
            BsonType.Int32 => typeof(int),
            BsonType.Int64 => typeof(long),
            BsonType.Double => typeof(double),
            BsonType.Decimal128 => typeof(decimal),
            BsonType.Boolean => typeof(bool),
            BsonType.String => typeof(string),
            _ => typeof(object)
        };
    }

    /// <summary>
    /// Fills a multidimensional array from flattened data
    /// </summary>
    private static int FillMultidimensionalArray(Array array, BsonArray data, int[] indices, int dimension, int dataIndex)
    {
        if (dimension == array.Rank)
        {
            if (dataIndex < data.Count)
            {
                object value = ConvertBsonValueToObject(data[dataIndex]);
                Type elementType = array.GetType().GetElementType();

                if (value != null && elementType != null && !elementType.IsAssignableFrom(value.GetType()))
                {
                    try
                    {
                        value = Convert.ChangeType(value, elementType);
                    }
                    catch
                    {
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

    #endregion

    #region Entity Serialization

    /// <summary>
    /// Serializes an entity to BsonDocument
    /// </summary>
    public static BsonDocument SerializeToDocument<T>(T obj)
    {
        if (obj == null)
            return new BsonDocument();

        try
        {
            return obj.ToBsonDocument();
        }
        catch
        {
            // Fallback to manual serialization
            var document = new BsonDocument();
            var properties = typeof(T).GetProperties();

            foreach (var prop in properties)
            {
                try
                {
                    var value = prop.GetValue(obj);
                    if (value != null)
                    {
                        document[prop.Name] = ConvertToBsonValue(value);
                    }
                }
                catch
                {
                    // Skip properties that throw
                }
            }

            return document;
        }
    }

    /// <summary>
    /// Deserializes a BsonDocument to entity
    /// </summary>
    public static T DeserializeFromDocument<T>(BsonDocument document)
    {
        if (document == null)
            return default;

        try
        {
            return BsonSerializer.Deserialize<T>(document);
        }
        catch
        {
            // Return default if deserialization fails
            return default;
        }
    }

    #endregion
}

#region Universal Serialization Provider

/// <summary>
/// Universal serialization provider - automatically handles ALL problematic types
/// - Dictionary<Enum, T> (any enum key type)
/// - ConcurrentDictionary<K, V> (any key/value types)
/// This is the MAGIC that makes everything work like LiteDB's BsonMapper.Global
/// </summary>
public class UniversalSerializationProvider : IBsonSerializationProvider
{
    public IBsonSerializer GetSerializer(Type type)
    {
        // Handle Dictionary<Enum, T> - any enum key type
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            var keyType = type.GetGenericArguments()[0];
            var valueType = type.GetGenericArguments()[1];

            if (keyType.IsEnum)
            {
                var serializerType = typeof(EnumKeyDictionarySerializer<,>).MakeGenericType(keyType, valueType);
                return (IBsonSerializer)Activator.CreateInstance(serializerType);
            }

            // NEW: Handle Dictionary<Guid, T> - any value type
            if (keyType == typeof(Guid))
            {
                var serializerType = typeof(GuidKeyDictionarySerializer<>).MakeGenericType(valueType);
                return (IBsonSerializer)Activator.CreateInstance(serializerType);
            }
        }

        // Handle ConcurrentDictionary<K, V>
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ConcurrentDictionary<,>))
        {
            var keyType = type.GetGenericArguments()[0];
            var valueType = type.GetGenericArguments()[1];
            var serializerType = typeof(ConcurrentDictionarySerializer<,>).MakeGenericType(keyType, valueType);
            return (IBsonSerializer)Activator.CreateInstance(serializerType);
        }

        return null; // Let default serializers handle other types
    }
}

#endregion

#region Custom Serializers for Dictionary<string, object>

/// <summary>
/// Custom serializer for Dictionary<string, object> that properly handles Guids
/// This is equivalent to LiteDB's BsonMapper.RegisterType for dictionaries
/// Solves: "GuidSerializer cannot serialize a Guid when GuidRepresentation is Unspecified"
/// </summary>
public class DictionaryWithGuidSerializer : SerializerBase<Dictionary<string, object>>
{
    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Dictionary<string, object> value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        context.Writer.WriteStartDocument();

        foreach (var kvp in value)
        {
            context.Writer.WriteName(kvp.Key);
            SerializeValue(context, kvp.Value);
        }

        context.Writer.WriteEndDocument();
    }

    private void SerializeValue(BsonSerializationContext context, object value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        switch (value)
        {
            case Guid guid:
                context.Writer.WriteBinaryData(new BsonBinaryData(guid, GuidRepresentation.Standard));
                break;

            case string str:
                context.Writer.WriteString(str);
                break;

            case int intVal:
                context.Writer.WriteInt32(intVal);
                break;

            case long longVal:
                context.Writer.WriteInt64(longVal);
                break;

            case double doubleVal:
                context.Writer.WriteDouble(doubleVal);
                break;

            case float floatVal:
                context.Writer.WriteDouble(floatVal);
                break;

            case decimal decimalVal:
                context.Writer.WriteDecimal128(new Decimal128(decimalVal));
                break;

            case bool boolVal:
                context.Writer.WriteBoolean(boolVal);
                break;

            case DateTime dateTimeVal:
                context.Writer.WriteDateTime(BsonUtils.ToMillisecondsSinceEpoch(dateTimeVal));
                break;

            case DateTimeOffset dtoVal:
                context.Writer.WriteDateTime(BsonUtils.ToMillisecondsSinceEpoch(dtoVal.UtcDateTime));
                break;

            case byte[] bytes:
                context.Writer.WriteBinaryData(new BsonBinaryData(bytes));
                break;

            case ObjectId oid:
                context.Writer.WriteObjectId(oid);
                break;

            case Dictionary<string, object> nestedDict:
                Serialize(context, new BsonSerializationArgs(), nestedDict);
                break;

            case IDictionary<string, object> iDict:
                Serialize(context, new BsonSerializationArgs(), new Dictionary<string, object>(iDict));
                break;

            case Array array:
                SerializeArray(context, array);
                break;

            case IList list:
                SerializeList(context, list);
                break;

            case IEnumerable enumerable when !(value is string):
                SerializeEnumerable(context, enumerable);
                break;

            default:
                try
                {
                    var serializer = BsonSerializer.LookupSerializer(value.GetType());
                    serializer.Serialize(context, value);
                }
                catch
                {
                    context.Writer.WriteString(value.ToString());
                }
                break;
        }
    }

    private void SerializeArray(BsonSerializationContext context, Array array)
    {
        context.Writer.WriteStartArray();
        foreach (var item in array)
        {
            SerializeValue(context, item);
        }
        context.Writer.WriteEndArray();
    }

    private void SerializeList(BsonSerializationContext context, IList list)
    {
        context.Writer.WriteStartArray();
        foreach (var item in list)
        {
            SerializeValue(context, item);
        }
        context.Writer.WriteEndArray();
    }

    private void SerializeEnumerable(BsonSerializationContext context, IEnumerable enumerable)
    {
        context.Writer.WriteStartArray();
        foreach (var item in enumerable)
        {
            SerializeValue(context, item);
        }
        context.Writer.WriteEndArray();
    }

    public override Dictionary<string, object> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var bsonType = context.Reader.GetCurrentBsonType();

        if (bsonType == BsonType.Null)
        {
            context.Reader.ReadNull();
            return null;
        }

        var dict = new Dictionary<string, object>();
        context.Reader.ReadStartDocument();

        while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
        {
            var key = context.Reader.ReadName();
            var value = DeserializeValue(context);
            dict[key] = value;
        }

        context.Reader.ReadEndDocument();
        return dict;
    }

    private object DeserializeValue(BsonDeserializationContext context)
    {
        var bsonType = context.Reader.GetCurrentBsonType();

        return bsonType switch
        {
            BsonType.Null => DeserializeNull(context),
            BsonType.String => context.Reader.ReadString(),
            BsonType.Int32 => context.Reader.ReadInt32(),
            BsonType.Int64 => context.Reader.ReadInt64(),
            BsonType.Double => context.Reader.ReadDouble(),
            BsonType.Decimal128 => Decimal128.ToDecimal(context.Reader.ReadDecimal128()),
            BsonType.Boolean => context.Reader.ReadBoolean(),
            BsonType.DateTime => BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(context.Reader.ReadDateTime()),
            BsonType.Binary => DeserializeBinary(context),
            BsonType.ObjectId => context.Reader.ReadObjectId(),
            BsonType.Document => Deserialize(context, new BsonDeserializationArgs()),
            BsonType.Array => DeserializeArray(context),
            _ => DeserializeDefault(context)
        };
    }

    private object DeserializeNull(BsonDeserializationContext context)
    {
        context.Reader.ReadNull();
        return null;
    }

    private object DeserializeBinary(BsonDeserializationContext context)
    {
        var binaryData = context.Reader.ReadBinaryData();

        if (binaryData.SubType == BsonBinarySubType.UuidStandard ||
            binaryData.SubType == BsonBinarySubType.UuidLegacy)
        {
            return binaryData.ToGuid();
        }

        return binaryData.Bytes;
    }

    private List<object> DeserializeArray(BsonDeserializationContext context)
    {
        var list = new List<object>();
        context.Reader.ReadStartArray();

        while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
        {
            list.Add(DeserializeValue(context));
        }

        context.Reader.ReadEndArray();
        return list;
    }

    private object DeserializeDefault(BsonDeserializationContext context)
    {
        try
        {
            return BsonSerializer.Deserialize<object>(context.Reader);
        }
        catch
        {
            context.Reader.SkipValue();
            return null;
        }
    }
}

/// <summary>
/// Custom serializer for IDictionary<string, object> interface
/// </summary>
public class IDictionaryWithGuidSerializer : SerializerBase<IDictionary<string, object>>
{
    private readonly DictionaryWithGuidSerializer _innerSerializer = new();

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, IDictionary<string, object> value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        var dict = value as Dictionary<string, object> ?? new Dictionary<string, object>(value);
        _innerSerializer.Serialize(context, args, dict);
    }

    public override IDictionary<string, object> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        return _innerSerializer.Deserialize(context, args);
    }
}

#endregion

#region Enum Key Dictionary Serializer

/// <summary>
/// Generic serializer for Dictionary with enum keys - handles ANY enum type automatically
/// Examples: Dictionary<ArgsValue, string>, Dictionary<ProfileCharacteristic, Guid>
/// </summary>
public class EnumKeyDictionarySerializer<TKey, TValue> : SerializerBase<Dictionary<TKey, TValue>>
    where TKey : struct, Enum
{
    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Dictionary<TKey, TValue> value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        context.Writer.WriteStartDocument();
        foreach (var kvp in value)
        {
            context.Writer.WriteName(kvp.Key.ToString());
            WriteValue(context, kvp.Value);
        }
        context.Writer.WriteEndDocument();
    }

    private void WriteValue(BsonSerializationContext context, TValue value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        switch (value)
        {
            case Guid g:
                context.Writer.WriteBinaryData(new BsonBinaryData(g, GuidRepresentation.Standard));
                break;
            case string s:
                context.Writer.WriteString(s);
                break;
            case int i:
                context.Writer.WriteInt32(i);
                break;
            case long l:
                context.Writer.WriteInt64(l);
                break;
            case double d:
                context.Writer.WriteDouble(d);
                break;
            case decimal dec:
                context.Writer.WriteDecimal128(new Decimal128(dec));
                break;
            case bool b:
                context.Writer.WriteBoolean(b);
                break;
            case DateTime dt:
                context.Writer.WriteDateTime(BsonUtils.ToMillisecondsSinceEpoch(dt));
                break;
            default:
                var serializer = BsonSerializer.LookupSerializer(typeof(TValue));
                serializer.Serialize(context, value);
                break;
        }
    }

    public override Dictionary<TKey, TValue> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var bsonType = context.Reader.GetCurrentBsonType();

        if (bsonType == BsonType.Null)
        {
            context.Reader.ReadNull();
            return null;
        }

        var dict = new Dictionary<TKey, TValue>();
        context.Reader.ReadStartDocument();

        while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
        {
            var keyStr = context.Reader.ReadName();
            if (Enum.TryParse<TKey>(keyStr, out var key))
            {
                dict[key] = ReadValue(context);
            }
            else
            {
                context.Reader.SkipValue();
            }
        }

        context.Reader.ReadEndDocument();
        return dict;
    }

    private TValue ReadValue(BsonDeserializationContext context)
    {
        var bsonType = context.Reader.GetCurrentBsonType();

        if (bsonType == BsonType.Null)
        {
            context.Reader.ReadNull();
            return default;
        }

        // Handle specific types
        if (typeof(TValue) == typeof(Guid) && bsonType == BsonType.Binary)
        {
            return (TValue)(object)context.Reader.ReadBinaryData().ToGuid();
        }
        if (typeof(TValue) == typeof(string) && bsonType == BsonType.String)
        {
            return (TValue)(object)context.Reader.ReadString();
        }
        if (typeof(TValue) == typeof(int) && bsonType == BsonType.Int32)
        {
            return (TValue)(object)context.Reader.ReadInt32();
        }
        if (typeof(TValue) == typeof(long) && bsonType == BsonType.Int64)
        {
            return (TValue)(object)context.Reader.ReadInt64();
        }
        if (typeof(TValue) == typeof(double) && bsonType == BsonType.Double)
        {
            return (TValue)(object)context.Reader.ReadDouble();
        }
        if (typeof(TValue) == typeof(bool) && bsonType == BsonType.Boolean)
        {
            return (TValue)(object)context.Reader.ReadBoolean();
        }
        if (typeof(TValue) == typeof(DateTime) && bsonType == BsonType.DateTime)
        {
            return (TValue)(object)BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(context.Reader.ReadDateTime());
        }

        // Fallback to generic deserialization
        var serializer = BsonSerializer.LookupSerializer(typeof(TValue));
        return (TValue)serializer.Deserialize(context);
    }
}

#endregion

#region ConcurrentDictionary Serializer

/// <summary>
/// Generic serializer for ConcurrentDictionary - handles ANY key/value types
/// Example: ConcurrentDictionary<string, ColumnProfile>
/// </summary>
public class ConcurrentDictionarySerializer<TKey, TValue> : SerializerBase<ConcurrentDictionary<TKey, TValue>>
{
    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, ConcurrentDictionary<TKey, TValue> value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        context.Writer.WriteStartDocument();
        foreach (var kvp in value)
        {
            context.Writer.WriteName(kvp.Key?.ToString() ?? "");

            if (kvp.Value == null)
            {
                context.Writer.WriteNull();
            }
            else
            {
                var serializer = BsonSerializer.LookupSerializer(typeof(TValue));
                serializer.Serialize(context, kvp.Value);
            }
        }
        context.Writer.WriteEndDocument();
    }

    public override ConcurrentDictionary<TKey, TValue> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var bsonType = context.Reader.GetCurrentBsonType();

        if (bsonType == BsonType.Null)
        {
            context.Reader.ReadNull();
            return null;
        }

        var dict = new ConcurrentDictionary<TKey, TValue>();
        context.Reader.ReadStartDocument();

        while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
        {
            var keyStr = context.Reader.ReadName();
            TKey key = ConvertKey(keyStr);

            TValue value;
            if (context.Reader.GetCurrentBsonType() == BsonType.Null)
            {
                context.Reader.ReadNull();
                value = default;
            }
            else
            {
                var serializer = BsonSerializer.LookupSerializer(typeof(TValue));
                value = (TValue)serializer.Deserialize(context);
            }

            dict[key] = value;
        }

        context.Reader.ReadEndDocument();
        return dict;
    }

    private TKey ConvertKey(string keyStr)
    {
        if (typeof(TKey) == typeof(string))
            return (TKey)(object)keyStr;

        if (typeof(TKey) == typeof(Guid))
            return (TKey)(object)Guid.Parse(keyStr);

        if (typeof(TKey).IsEnum)
            return (TKey)Enum.Parse(typeof(TKey), keyStr);

        return (TKey)Convert.ChangeType(keyStr, typeof(TKey));
    }
}

#endregion

#region Guid Key Dictionary Serializer

/// <summary>
/// Generic serializer for Dictionary with Guid keys - handles ANY value type automatically
/// Examples: Dictionary<Guid, bool>, Dictionary<Guid, long>, Dictionary<Guid, IList<CleaningRule>>
/// Equivalent to LiteDB's BsonConverter.RegisterGuidDictionaryType<TValue>
/// </summary>
public class GuidKeyDictionarySerializer<TValue> : SerializerBase<Dictionary<Guid, TValue>>
{
    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Dictionary<Guid, TValue> value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        context.Writer.WriteStartDocument();
        foreach (var kvp in value)
        {
            // Convert Guid key to string (same as LiteDB does)
            context.Writer.WriteName(kvp.Key.ToString());
            WriteValue(context, kvp.Value);
        }
        context.Writer.WriteEndDocument();
    }

    private void WriteValue(BsonSerializationContext context, TValue value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        // Handle common value types explicitly for performance
        switch (value)
        {
            case bool b:
                context.Writer.WriteBoolean(b);
                break;
            case long l:
                context.Writer.WriteInt64(l);
                break;
            case int i:
                context.Writer.WriteInt32(i);
                break;
            case string s:
                context.Writer.WriteString(s);
                break;
            case double d:
                context.Writer.WriteDouble(d);
                break;
            case decimal dec:
                context.Writer.WriteDecimal128(new Decimal128(dec));
                break;
            case Guid g:
                context.Writer.WriteBinaryData(new BsonBinaryData(g, GuidRepresentation.Standard));
                break;
            case DateTime dt:
                context.Writer.WriteDateTime(BsonUtils.ToMillisecondsSinceEpoch(dt));
                break;
            default:
                // Use MongoDB's serializer for complex types (lists, custom objects, etc.)
                var serializer = BsonSerializer.LookupSerializer(typeof(TValue));
                serializer.Serialize(context, value);
                break;
        }
    }

    public override Dictionary<Guid, TValue> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var bsonType = context.Reader.GetCurrentBsonType();

        if (bsonType == BsonType.Null)
        {
            context.Reader.ReadNull();
            return null;
        }

        var dict = new Dictionary<Guid, TValue>();
        context.Reader.ReadStartDocument();

        while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
        {
            var keyStr = context.Reader.ReadName();

            // Parse Guid key from string (same as LiteDB does)
            if (Guid.TryParse(keyStr, out var key))
            {
                dict[key] = ReadValue(context);
            }
            else
            {
                // Skip invalid Guid keys
                context.Reader.SkipValue();
            }
        }

        context.Reader.ReadEndDocument();
        return dict;
    }

    private TValue ReadValue(BsonDeserializationContext context)
    {
        var bsonType = context.Reader.GetCurrentBsonType();

        if (bsonType == BsonType.Null)
        {
            context.Reader.ReadNull();
            return default;
        }

        // Handle specific types for performance
        if (typeof(TValue) == typeof(bool) && bsonType == BsonType.Boolean)
        {
            return (TValue)(object)context.Reader.ReadBoolean();
        }
        if (typeof(TValue) == typeof(long) && bsonType == BsonType.Int64)
        {
            return (TValue)(object)context.Reader.ReadInt64();
        }
        if (typeof(TValue) == typeof(int) && (bsonType == BsonType.Int32 || bsonType == BsonType.Int64))
        {
            return (TValue)(object)(bsonType == BsonType.Int32
                ? context.Reader.ReadInt32()
                : (int)context.Reader.ReadInt64());
        }
        if (typeof(TValue) == typeof(string) && bsonType == BsonType.String)
        {
            return (TValue)(object)context.Reader.ReadString();
        }
        if (typeof(TValue) == typeof(double) && bsonType == BsonType.Double)
        {
            return (TValue)(object)context.Reader.ReadDouble();
        }
        if (typeof(TValue) == typeof(decimal) && bsonType == BsonType.Decimal128)
        {
            return (TValue)(object)Decimal128.ToDecimal(context.Reader.ReadDecimal128());
        }
        if (typeof(TValue) == typeof(Guid) && bsonType == BsonType.Binary)
        {
            return (TValue)(object)context.Reader.ReadBinaryData().ToGuid();
        }
        if (typeof(TValue) == typeof(DateTime) && bsonType == BsonType.DateTime)
        {
            return (TValue)(object)BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(context.Reader.ReadDateTime());
        }

        // Fallback to generic deserialization for complex types
        var serializer = BsonSerializer.LookupSerializer(typeof(TValue));
        return (TValue)serializer.Deserialize(context);
    }
}

#endregion