using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using xdeequ.Analyzers.Runners;

namespace xdeequ.Repository
{
    public class SimpleResultSerializer : JsonConverter<SimpleMetricOutput>
    {
        public override SimpleMetricOutput Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out JsonDocument document);
            string name = document.RootElement.GetProperty("name").GetString();
            string instance = document.RootElement.GetProperty("instance").GetString();
            string entity = document.RootElement.GetProperty("entity").GetString();
            double value = document.RootElement.GetProperty("value").GetDouble();

            return new SimpleMetricOutput { Name = name, Entity = entity, Instance = instance, Value = value };
        }

        public override void Write(Utf8JsonWriter writer, SimpleMetricOutput value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("name", value.Name);
            writer.WriteString("instance", value.Instance);
            writer.WriteString("entity", value.Entity);
            writer.WriteNumber("value", value.Value);
            writer.WriteEndObject();
        }
    }
}
