using System.Collections.Generic;
using System.Linq;
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
                AnalyzerContext.SuccessMetricsAsDataFrame(analysisResult.AnalyzerContext, sparkSession, forAnalyzer)
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

        public static DataFrame GetSuccessMetricsAsJson(this AnalysisResult analysisResult,
            SparkSession sparkSession,
            IEnumerable<IAnalyzer<IMetric>> forAnalyzer,
            IEnumerable<string> withTags
        )
        {
            DataFrame analyzerContextDF =
                AnalyzerContext.SuccessMetricsAsDataFrame(analysisResult.AnalyzerContext, sparkSession, forAnalyzer)
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

        private static string FormatTagColumnNameInDataFrame(string tagName, DataFrame dataFrame)
        {
            string tagColumnName = tagName.Replace("[^A-Za-z0-9_]", "").ToLowerInvariant();
            if (dataFrame.Columns().Contains(tagColumnName))
            {
                tagColumnName += "_2";
            }

            return tagColumnName;
        }
    }
}
