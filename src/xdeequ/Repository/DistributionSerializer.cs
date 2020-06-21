using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace xdeequ.Metrics
{
    public class DistributionSerializer : JsonConverter<Distribution>
    {
        public override Distribution Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out var document);

            var values = document.RootElement.GetProperty("values");
            var distributionValues = values.EnumerateArray().Select(x =>
               new KeyValuePair<string, DistributionValue>(x.GetProperty("key").GetString(),
                   new DistributionValue(x.GetProperty("absolute").GetInt64(), x.GetProperty("ratio").GetInt64())));

            return new Distribution(new Dictionary<string, DistributionValue>(distributionValues), document.RootElement.GetProperty("numberOfBins").GetInt64());
        }

        public override void Write(Utf8JsonWriter writer, Distribution distribution, JsonSerializerOptions options)
        {
            writer.WriteNumber("numberOfBins", distribution.NumberOfBins);

            writer.WriteStartArray("values");
            foreach (var distributionValue in distribution.Values)
            {
                writer.WriteString("key", distributionValue.Key);
                writer.WriteNumber("absolute", distributionValue.Value.Absolute);
                writer.WriteNumber("ratio", distributionValue.Value.Ratio);
            }

            writer.WriteEndArray();

        }
    }
}