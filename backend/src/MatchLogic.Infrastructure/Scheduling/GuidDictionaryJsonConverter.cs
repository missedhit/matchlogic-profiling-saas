using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace MatchLogic.Infrastructure.Scheduling;

/// <summary>
/// Newtonsoft.Json converter for Dictionary<string, object> that preserves Guid types
/// Used by Hangfire's JSON serialization
/// </summary>
public class GuidDictionaryJsonConverter : JsonConverter<Dictionary<string, object>>
{
    public override void WriteJson(JsonWriter writer, Dictionary<string, object> value, JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
            return;
        }

        writer.WriteStartObject();

        foreach (var kvp in value)
        {
            writer.WritePropertyName(kvp.Key);

            if (kvp.Value == null)
            {
                writer.WriteNull();
            }
            else if (kvp.Value is Guid guid)
            {
                // Write Guid with type marker
                writer.WriteStartObject();
                writer.WritePropertyName("$type");
                writer.WriteValue("System.Guid");
                writer.WritePropertyName("$value");
                writer.WriteValue(guid.ToString());
                writer.WriteEndObject();
            }
            else
            {
                serializer.Serialize(writer, kvp.Value);
            }
        }

        writer.WriteEndObject();
    }

    public override Dictionary<string, object> ReadJson(
        JsonReader reader,
        Type objectType,
        Dictionary<string, object> existingValue,
        bool hasExistingValue,
        JsonSerializer serializer)
    {
        if (reader.TokenType == JsonToken.Null)
            return null;

        var dict = new Dictionary<string, object>();
        var jObject = JObject.Load(reader);

        foreach (var property in jObject.Properties())
        {
            dict[property.Name] = ParseValue(property.Value);
        }

        return dict;
    }

    private object ParseValue(JToken token)
    {
        if (token == null || token.Type == JTokenType.Null)
            return null;

        // Check for Guid type marker
        if (token.Type == JTokenType.Object)
        {
            var obj = (JObject)token;

            if (obj.TryGetValue("$type", out var typeToken) &&
                typeToken.ToString() == "System.Guid" &&
                obj.TryGetValue("$value", out var valueToken))
            {
                return Guid.Parse(valueToken.ToString());
            }

            // Recursively parse nested objects
            var dict = new Dictionary<string, object>();
            foreach (var prop in obj.Properties())
            {
                dict[prop.Name] = ParseValue(prop.Value);
            }
            return dict;
        }

        if (token.Type == JTokenType.Array)
        {
            var list = new List<object>();
            foreach (var item in token)
            {
                list.Add(ParseValue(item));
            }
            return list;
        }

        // Try parsing string as Guid
        if (token.Type == JTokenType.String)
        {
            var str = token.ToString();
            if (Guid.TryParse(str, out var guid))
                return guid;

            if (DateTime.TryParse(str, out var dateTime))
                return dateTime;

            return str;
        }

        // Primitive types
        return token.ToObject<object>();
    }
}