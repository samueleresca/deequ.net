using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Repository
{
    public class MetricSerializer : JsonConverter<IMetric>
    {
        public override IMetric Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out JsonDocument document);
            string metricName = document.RootElement.GetProperty("metricName").GetString();

            if (metricName == "DoubleMetric")
            {
                return new DoubleMetric(
                    Entity.Column, //.RootElement.GetProperty("entity").GetString();
                    document.RootElement.GetProperty("name").GetString(),
                    document.RootElement.GetProperty("instance").GetString(),
                    document.RootElement.GetProperty("value").GetDouble()
                );
            }

            if (metricName == "HistogramMetric")
            {
                return new HistogramMetric(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    new Try<Distribution>(
                        JsonSerializer.Deserialize<Distribution>(document.RootElement.GetProperty("value").GetString(),
                            options)
                    ));
            }

            throw new ArgumentException($"Unable to deserialize analyzer {metricName}.");
        }

        public override void Write(Utf8JsonWriter writer, IMetric value, JsonSerializerOptions options)
        {
            if (value is DoubleMetric dm)
            {
                if (!dm.Value.IsSuccess)
                {
                    throw new ArgumentException("Unable to serialize failed metrics.");
                }

                writer.WriteString("metricName", "DoubleMetric");
                writer.WriteString("entity", dm.Entity.ToString());
                writer.WriteString("instance", dm.Instance);
                writer.WriteString("name", dm.Name);
                writer.WriteString("value", dm.Value.GetOrElse(null).ToString());

                return;
            }

            if (value is HistogramMetric hm)
            {
                if (!hm.Value.IsSuccess)
                {
                    throw new ArgumentException("Unable to serialize failed metrics.");
                }

                writer.WriteString("metricName", "HistogramMetric");
                writer.WriteString(SerdeExt.COLUMN_FIELD, hm.Column);
                writer.WriteString("numberOfBins", hm.Value.Get().NumberOfBins.ToString());
                writer.WriteString("value", JsonSerializer.Serialize(hm.Value.GetOrElse(null), options));
            }

            //TODO: implement keyed double metric
        }
    }
}
