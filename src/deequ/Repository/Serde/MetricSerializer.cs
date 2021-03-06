using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;

namespace deequ.Repository.Serde
{
    internal class MetricSerializer : JsonConverter<IMetric>
    {
        public override IMetric Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out JsonDocument document);
            string metricName = document.RootElement.GetProperty("metricName").GetString();

            if (metricName == "DoubleMetric")
            {
                return new DoubleMetric(
                    (MetricEntity)Enum.Parse(typeof(MetricEntity), document.RootElement.GetProperty("entity").GetString()),
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
                        JsonSerializer.Deserialize<Distribution>(document.RootElement.GetProperty("value").GetRawText(),
                            options)
                    ));
            }

            throw new ArgumentException($"Unable to deserialize analyzer {metricName}.");
        }

        public override void Write(Utf8JsonWriter writer, IMetric value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            if (value is DoubleMetric dm)
            {
                if (!dm.Value.IsSuccess)
                {
                    throw new ArgumentException("Unable to serialize failed metrics.");
                }

                writer.WriteString("metricName", "DoubleMetric");
                writer.WriteString("entity", Enum.GetName(typeof(MetricEntity), dm.MetricEntity));
                writer.WriteString("instance", dm.Instance);
                writer.WriteString("name", dm.Name);
                writer.WriteNumber("value", dm.Value.GetOrElse(() => 0D).Get());
            }

            if (value is HistogramMetric hm)
            {
                if (!hm.Value.IsSuccess)
                {
                    throw new ArgumentException("Unable to serialize failed metrics.");
                }

                writer.WriteString("metricName", "HistogramMetric");
                writer.WriteString(SerdeExt.COLUMN_FIELD, hm.Column.GetOrElse(string.Empty));
                writer.WriteString("numberOfBins", hm.Value.Get().NumberOfBins.ToString());
                writer.WritePropertyName("value");
                JsonSerializer.Serialize(writer, hm.Value.GetOrElse(null).Get(), options);
            }

            //TODO: implement keyed double metric

            writer.WriteEndObject();
        }
    }
}
