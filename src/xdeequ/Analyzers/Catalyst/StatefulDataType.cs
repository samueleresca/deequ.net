using System;
using System.Collections.ObjectModel;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers.Catalyst
{
    public class StatefulDataType
    {
        const int SIZE_IN_BITES = 40;
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
                new StructField("null", new LongType()),
                new StructField("fractional", new LongType()),
                new StructField("integral", new LongType()),
                new StructField("boolean", new LongType()),
                new StructField("string", new LongType())
            });
        }

        public Row Update(Row row)
        {
            object[] values = new object[SIZE_IN_BITES];

            if (row[0] == null)
            {
                values[NULL_POS] = (long)values[NULL_POS] + 1L;
                return new GenericRow(values);
            }

            string value = (string)row.GetAs<string>(0);

            if (FRACTIONAL.IsMatch(value))
                values[FRATIONAL_POS] = (long)values[FRATIONAL_POS] + 1L;

            if (INTEGRAL.IsMatch(value))
                values[INTEGRAL_POS] = (long)values[INTEGRAL_POS] + 1L;

            if (BOOLEAN.IsMatch(value))
                values[BOOLEAN_POS] = (long)values[BOOLEAN_POS] + 1L;

            values[STRING_POS] = (long)values[STRING_POS] + 1L;

            return new GenericRow(values);
        }

        public Row Merge(Row buffer1, Row buffer2)
        {
            object[] values1 = buffer1.Values;
            object[] values2 = buffer2.Values;

            values1[NULL_POS] = (long)values1[NULL_POS] + (long)values2[NULL_POS];
            values1[FRATIONAL_POS] = (long)values1[FRATIONAL_POS] + (long)values2[FRATIONAL_POS];
            values1[INTEGRAL_POS] = (long)values1[INTEGRAL_POS] + (long)values2[INTEGRAL_POS];
            values1[BOOLEAN_POS] = (long)values1[BOOLEAN_POS] + (long)values2[BOOLEAN_POS];
            values1[STRING_POS] = (long)values1[STRING_POS] + (long)values2[STRING_POS];

            return new GenericRow(values1);
        }

        public ReadOnlySpan<byte> Evaluate(Row buffer)
        {
            return DataTypeHistogram.ToBytes(buffer.GetAs<long>(NULL_POS),
                buffer.GetAs<long>(FRATIONAL_POS), buffer.GetAs<long>(INTEGRAL_POS), buffer.GetAs<long>(BOOLEAN_POS),
                buffer.GetAs<long>(STRING_POS));
        }
    }
}