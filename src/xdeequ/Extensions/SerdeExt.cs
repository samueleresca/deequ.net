using System.Collections.Generic;
using System.Text.Json;

namespace xdeequ.Metrics
{
    public static class SerdeExt
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
    }
}
