using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Metrics;
using deequ.Repository;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Extensions
{
    internal static class AnalysisResultExt
    {
        public static DataFrame GetSuccessMetricsAsDataFrame(this AnalysisResult analysisResult,
            SparkSession sparkSession,
            IEnumerable<IAnalyzer<IMetric>> forAnalyzer,
            IEnumerable<string> withTags
        )
        {
            DataFrame analyzerContextDF =
                analysisResult.AnalyzerContext.SuccessMetricsAsDataFrame(sparkSession, forAnalyzer)
                    .WithColumn("dataset_date", Lit(analysisResult.ResultKey.DataSetDate));

            IEnumerable<KeyValuePair<string, string>> analyzerTags = analysisResult.ResultKey.Tags
                .Where(pair => !withTags.Any() || withTags.Contains(pair.Key))
                .Select(keyValue =>
                    new KeyValuePair<string, string>(FormatTagColumnNameInDataFrame(keyValue.Key, analyzerContextDF),
                        keyValue.Value));

            foreach (KeyValuePair<string, string> tag in analyzerTags)
            {
                analyzerContextDF = analyzerContextDF.WithColumn(tag.Key, Lit(tag.Value));
            }

            return analyzerContextDF;
        }

        public static string GetSuccessMetricsAsJson(this AnalysisResult analysisResult,
            IEnumerable<IAnalyzer<IMetric>> forAnalyzer,
            IEnumerable<string> withTags
        )
        {
            SimpleMetricOutput[] serializableResult = JsonSerializer
                .Deserialize<SimpleMetricOutput[]>(
                    analysisResult.AnalyzerContext.SuccessMetricsAsJson(forAnalyzer), SerdeExt.GetDefaultOptions());

            IEnumerable<Dictionary<string, object>> enanchedResult = ConvertAndAddColumnToSerializableResult(
                serializableResult, SerdeExt.DATASET_DATE_FIELD, analysisResult.ResultKey.DataSetDate);

            foreach ((string key, string value) in analysisResult.ResultKey.Tags.Where(
                keyValue => withTags.Any()
                     && !withTags.Contains(keyValue.Key)
            ).Select(keyValue => (FormatTagColumnNameInJson(keyValue.Key, enanchedResult), keyValue.Value)))
            {
                enanchedResult = AddColumnToSerializableResult(enanchedResult, key, value);
            }

            return JsonSerializer.Serialize(enanchedResult.ToArray(), SerdeExt.GetDefaultOptions());
        }

        private static IEnumerable<Dictionary<string, object>> ConvertAndAddColumnToSerializableResult(
            IEnumerable<SimpleMetricOutput> serializableResult,
            string tagName, object serializableTagValue)
        {
            IEnumerable<Dictionary<string, object>> fields = serializableResult.Select(simpleMetric =>
                new Dictionary<string, object>
                {
                    {"name", simpleMetric.Name}, {"instance", simpleMetric.Instance}, {"entity", simpleMetric.Entity}, {"value", simpleMetric.Value}
                });

            return AddColumnToSerializableResult(fields, tagName, serializableTagValue);
        }

        private static IEnumerable<Dictionary<string, object>> AddColumnToSerializableResult(
            IEnumerable<Dictionary<string, object>> fields, string tagName, object serializableTagValue
        )
        {
            if (fields.FirstOrDefault() != null && fields.All(dictionary => !dictionary.ContainsKey(tagName)))
            {
                fields = fields.Select(dict =>
                {
                    dict.Add(tagName, serializableTagValue);
                    return dict;
                });

                return fields;
            }

            return fields;
        }


        private static string FormatTagColumnNameInDataFrame(string tagName, DataFrame dataFrame)
        {
            string tagColumnName = tagName.Replace("[^A-Za-z0-9_]", "").ToLowerInvariant();
            if (dataFrame.Columns().Contains(tagColumnName))
            {
                tagColumnName += "_2";
            }

            return tagColumnName;
        }

        private static string FormatTagColumnNameInJson(string tagName,
            IEnumerable<Dictionary<string, object>> sequence)
        {
            string tagColumnName = tagName.Replace("[^A-Za-z0-9_]", "").ToLowerInvariant();

            if (sequence.Any() && sequence.Any(dict => dict.ContainsKey(tagColumnName)))
            {
                tagColumnName += "_2";
            }

            return tagColumnName;
        }
    }
}
