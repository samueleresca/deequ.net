using System;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;


namespace xdeequ.Analyzers.Catalyst
{
    [Serializable]
    public class StatefulDataType
    {
        const int SIZE_IN_BITES = 5;
        const int NULL_POS = 0;
        const int FRATIONAL_POS = 1;
        const int INTEGRAL_POS = 2;
        const int BOOLEAN_POS = 3;
        const int STRING_POS = 4;

        public static Regex FRACTIONAL = new Regex(@"^(-|\+)? ?\d*\.\d*$");
        public static Regex INTEGRAL = new Regex(@"^(-|\+)? ?\d*$");
        public static Regex BOOLEAN = new Regex(@"^(true|false)$");

        public StructType InputSchema() => new StructType(new[] { new StructField("value", new StringType()) });
        public BinaryType DataType() => new BinaryType();
        public bool Deterministic() => true;
        public Row Initialize() => new GenericRow(new object[] { 0L, 0L, 0L, 0L, 0L });

        public StructType BufferSchema()
        {
            return new StructType(new[]
            {
                new StructField("nullCount", new LongType()),
                new StructField("fractionalCount", new LongType()),
                new StructField("integralCount", new LongType()),
                new StructField("booleanCount", new LongType()),
                new StructField("stringCount", new LongType())
            });
        }

        public string GetAggregatedColumn() => "arrayDataTypeCount";

        public string[] ColumnNames() => new[]
        {
            "nullCount",
            "fractionalCount",
            "integralCount",
            "booleanCount",
            "stringCount",
        };

        public int[] Update(string columnValue)
        {
            int[] values = new int[SIZE_IN_BITES];

            if (columnValue == null)
            {
                values[NULL_POS] = values[NULL_POS] + 1;
                return values;
            }

            string value = columnValue;

            if (FRACTIONAL.IsMatch(value))
                values[FRATIONAL_POS] = values[FRATIONAL_POS] + 1;
            else if (INTEGRAL.IsMatch(value))
                values[INTEGRAL_POS] = values[INTEGRAL_POS] + 1;
            else if (BOOLEAN.IsMatch(value))
                values[BOOLEAN_POS] = values[BOOLEAN_POS] + 1;
            else values[STRING_POS] = values[STRING_POS] + 1;

            return values;
        }
    }
}