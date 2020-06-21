using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using xdeequ.Analyzers;

namespace xdeequ.Metrics
{
    public class MetricSerializer : JsonConverter<IAnalyzer<IMetric>>
    {
        public override IAnalyzer<IMetric> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }

        public override void Write(Utf8JsonWriter writer, IAnalyzer<IMetric> value, JsonSerializerOptions options)
        {
            throw new NotImplementedException();
        }
    }
}