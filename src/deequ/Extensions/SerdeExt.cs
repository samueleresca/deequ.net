using System.Collections.Generic;
using System.Text.Encodings.Web;
using System.Text.Json;
using deequ.Repository.Serde;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Extensions
{
    internal static class SerdeExt
    {
        public static string ANALYZER_FIELD = "analyzer";
        public static string ANALYZER_NAME_FIELD = "analyzerName";
        public static string WHERE_FIELD = "where";
        public static string COLUMN_FIELD = "column";
        public static string COLUMNS_FIELD = "columns";
        public static string METRIC_MAP_FIELD = "metricMap";
        public static string METRIC_FIELD = "metric";
        public static string DATASET_DATE_FIELD = "dataSetDate";
        public static string TAGS_FIELD = "tags";
        public static string RESULT_KEY_FIELD = "resultKey";
        public static string ANALYZER_CONTEXT_FIELD = "analyzerContext";

        public static void WriteArray(this Utf8JsonWriter writer, string fieldName, IEnumerable<string> values)
        {
            writer.WriteStartArray(COLUMNS_FIELD);
            foreach (string val in values)
            {
                writer.WriteStringValue(val);
            }

            writer.WriteEndArray();
        }

        public static JsonSerializerOptions GetDefaultOptions()
        {
            JsonSerializerOptions serializeOptions = new JsonSerializerOptions();
            serializeOptions.Converters.Add(new AnalyzerSerializer());
            serializeOptions.Converters.Add(new AnalysisResultSerializer());
            serializeOptions.Converters.Add(new AnalyzerContextSerializer());
            serializeOptions.Converters.Add(new MetricSerializer());
            serializeOptions.Converters.Add(new DistributionSerializer());
            serializeOptions.Converters.Add(new SimpleResultSerializer());

            serializeOptions.WriteIndented = true;

            return serializeOptions;
        }

        public static DataFrame ToDataFrame(this IEnumerable<SimpleCheckResultOutput> simpleChecks)
        {
            List<GenericRow> elements = new List<GenericRow>();

            foreach (SimpleCheckResultOutput check in simpleChecks)
            {
                elements.Add(
                    new GenericRow(new[]
                    {
                        check.CheckDescription, check.CheckLevel, check.CheckStatus, check.Constraint,
                        check.ConstraintStatus, check.ConstraintMessage
                    }));
            }

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("check", new StringType()),
                    new StructField("check_level", new StringType()),
                    new StructField("check_status", new StringType()),
                    new StructField("constraint", new StringType()),
                    new StructField("constraint_status", new StringType()),
                    new StructField("constraint_message", new StringType())
                });

            return SparkSession.Active().CreateDataFrame(elements, schema);
        }
    }
}
