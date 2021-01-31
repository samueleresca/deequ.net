using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using deequ.Metrics;

namespace deequ.Repository.Serde
{
    internal class DistributionSerializer : JsonConverter<Distribution>
    {
        public override Distribution Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out JsonDocument document);

            JsonElement values = document.RootElement.GetProperty("values");
            IEnumerable<KeyValuePair<string, DistributionValue>> distributionValues = values.EnumerateArray().Select(
                jsonElement =>
                    new KeyValuePair<string, DistributionValue>(jsonElement.GetProperty("key").GetString(),
                        new DistributionValue(jsonElement.GetProperty("absolute").GetInt64(),
                            jsonElement.GetProperty("ratio").GetDouble())));

            return new Distribution(new Dictionary<string, DistributionValue>(distributionValues),
                document.RootElement.GetProperty("numberOfBins").GetInt64());
        }

        public override void Write(Utf8JsonWriter writer, Distribution distributionJvm, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            writer.WriteNumber("numberOfBins", distributionJvm.NumberOfBins);

            writer.WriteStartArray("values");
            foreach (KeyValuePair<string, DistributionValue> distributionValue in distributionJvm.Values)
            {
                writer.WriteStartObject();
                writer.WriteString("key", distributionValue.Key);
                writer.WriteNumber("absolute", distributionValue.Value.Absolute);
                writer.WriteNumber("ratio", distributionValue.Value.Ratio);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();

            writer.WriteEndObject();
        }
    }
}
