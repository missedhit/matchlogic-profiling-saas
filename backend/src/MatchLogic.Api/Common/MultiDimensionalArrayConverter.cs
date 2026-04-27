using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace MatchLogic.Api.Common;
public class MultiDimensionalArrayConverter<T> : JsonConverter<T[,]>
{
    public override T[,]? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartArray)
            throw new JsonException("Expected StartArray token");

        var rows = new List<List<T>>();

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                break;

            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException("Expected StartArray token inside outer array");

            var row = new List<T>();

            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndArray)
                    break;

                row.Add(JsonSerializer.Deserialize<T>(ref reader, options)!);
            }

            rows.Add(row);
        }

        if (rows.Count == 0)
            return new T[0, 0];

        int height = rows.Count;
        int width = rows[0].Count;

        var result = new T[height, width];

        for (int i = 0; i < height; i++)
        {
            if (rows[i].Count != width)
                throw new JsonException("Jagged array is not supported in multi-dimensional arrays");

            for (int j = 0; j < width; j++)
            {
                result[i, j] = rows[i][j];
            }
        }

        return result;
    }

    public override void Write(Utf8JsonWriter writer, T[,] value, JsonSerializerOptions options)
    {
        writer.WriteStartArray();

        int height = value.GetLength(0);
        int width = value.GetLength(1);

        for (int i = 0; i < height; i++)
        {
            writer.WriteStartArray();
            for (int j = 0; j < width; j++)
            {
                JsonSerializer.Serialize(writer, value[i, j], options);
            }
            writer.WriteEndArray();
        }

        writer.WriteEndArray();
    }
}

