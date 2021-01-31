using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Extensions;
using deequ.Metrics;

namespace deequ.Repository.Serde
{
    internal class AnalyzerContextSerializer : JsonConverter<AnalyzerContext>
    {
        public override AnalyzerContext Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out JsonDocument document);
            JsonElement.ArrayEnumerator metricMap =
                document.RootElement.GetProperty(SerdeExt.METRIC_MAP_FIELD).EnumerateArray();

            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> result = metricMap.Select(element =>
            {
                JsonElement serializedAnalyzer = element.GetProperty(SerdeExt.ANALYZER_FIELD);
                IAnalyzer<IMetric> analyzer =
                    JsonSerializer.Deserialize<IAnalyzer<IMetric>>(serializedAnalyzer.GetRawText(), options);

                JsonElement serializedMetric = element.GetProperty(SerdeExt.METRIC_FIELD);
                IMetric metric = JsonSerializer.Deserialize<IMetric>(serializedMetric.GetRawText(), options);

                return new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer, metric);
            });

            return new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(result));
        }

        public override void Write(Utf8JsonWriter writer, AnalyzerContext value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteStartArray(SerdeExt.METRIC_MAP_FIELD);

            foreach (KeyValuePair<IAnalyzer<IMetric>, IMetric> keyValuePair in value.MetricMap().ToDictionary<IAnalyzer<IMetric>, IMetric>())
            {
                writer.WriteStartObject();

                writer.WritePropertyName(SerdeExt.ANALYZER_FIELD);
                JsonSerializer.Serialize(writer, keyValuePair.Key, options);

                writer.WritePropertyName(SerdeExt.METRIC_FIELD);
                JsonSerializer.Serialize(writer, keyValuePair.Value, options);

                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }
    }
}
