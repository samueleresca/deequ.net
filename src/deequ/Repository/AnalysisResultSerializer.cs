using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using xdeequ.Analyzers.Runners;
using xdeequ.Extensions;

namespace xdeequ.Repository
{
    public class AnalysisResultSerializer : JsonConverter<AnalysisResult>
    {
        public override AnalysisResult Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out JsonDocument document);
            JsonElement resultKey = document.RootElement.GetProperty(SerdeExt.RESULT_KEY_FIELD);
            JsonElement analyzer = document.RootElement.GetProperty(SerdeExt.ANALYZER_CONTEXT_FIELD);


            ResultKey resultKeyDe = JsonSerializer.Deserialize<ResultKey>(resultKey.GetRawText(), options);
            AnalyzerContext analyzerContextDe =
                JsonSerializer.Deserialize<AnalyzerContext>(analyzer.GetRawText(), options);

            return new AnalysisResult(resultKeyDe, analyzerContextDe);
        }

        public override void Write(Utf8JsonWriter writer, AnalysisResult result, JsonSerializerOptions options)
        {
            writer.WriteStartObject();


            writer.WritePropertyName(SerdeExt.RESULT_KEY_FIELD);
            JsonSerializer.Serialize(writer, result.ResultKey, options);

            writer.WritePropertyName(SerdeExt.ANALYZER_CONTEXT_FIELD);
            JsonSerializer.Serialize(writer, result.AnalyzerContext, options);

            writer.WriteEndObject();
        }
    }
}
