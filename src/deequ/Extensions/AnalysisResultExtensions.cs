using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Metrics;
using xdeequ.Repository;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Extensions
{
    public static class AnalysisResultExtensions
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
                .Select(x =>
                    new KeyValuePair<string, string>(FormatTagColumnNameInDataFrame(x.Key, analyzerContextDF),
                        x.Value));

            foreach (KeyValuePair<string, string> tag in analyzerTags)
            {
                analyzerContextDF = analyzerContextDF.WithColumn(tag.Key, Lit(tag.Value));
            }

            return analyzerContextDF;
        }

        public static string GetSuccessMetricsAsJson(this AnalysisResult analysisResult,
            SparkSession sparkSession,
            IEnumerable<IAnalyzer<IMetric>> forAnalyzer,
            IEnumerable<string> withTags
        )
        {
            SimpleMetricOutput[] serializableResult = JsonSerializer
                .Deserialize<SimpleMetricOutput[]>(
                    analysisResult.AnalyzerContext.SuccessMetricsAsJson(forAnalyzer));

            IEnumerable<Dictionary<string, object>> enanchedResult = ConvertAndAddColumnToSerializableResult(
                serializableResult, SerdeExt.DATASET_DATE_FIELD, analysisResult.ResultKey.DataSetDate);

            foreach ((string key, string value) in analysisResult.ResultKey.Tags.Where(
                x => withTags.Any()
                     && !withTags.Contains(x.Key)
            ).Select(x => (FormatTagColumnNameInJson(x.Key, enanchedResult), x.Value)))
            {
                enanchedResult = AddColumnToSerializableResult(enanchedResult, key, value);
            }

            return JsonSerializer.Serialize(enanchedResult.ToArray());
        }

        private static IEnumerable<Dictionary<string, object>> ConvertAndAddColumnToSerializableResult(
            IEnumerable<SimpleMetricOutput> serializableResult,
            string tagName, object serializableTagValue)
        {
            IEnumerable<Dictionary<string, object>> fields = serializableResult.Select(x =>
                new Dictionary<string, object>
                {
                    {"name", x.Name}, {"instance", x.Instance}, {"entity", x.Entity}, {"value", x.Value}
                });

            return AddColumnToSerializableResult(fields, tagName, serializableTagValue);
        }

        private static IEnumerable<Dictionary<string, object>> AddColumnToSerializableResult(
            IEnumerable<Dictionary<string, object>> fields, string tagName, object serializableTagValue
        )
        {
            if (fields.FirstOrDefault() != null && fields.All(x => !x.ContainsKey(tagName)))
            {
                fields = fields.Select(x =>
                {
                    x.Add(tagName, serializableTagValue);
                    return x;
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

            if (sequence.Any() && sequence.Any(x => x.ContainsKey(tagColumnName)))
            {
                tagColumnName += "_2";
            }

            return tagColumnName;
        }
    }
}