using System.Collections.Generic;
using System.Linq;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using Xunit.Abstractions;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.tests
{
    public static class FixtureSupport
    {
        public static DataFrame GetDFMissing(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "a", "f"}),
                new GenericRow(new object[] {"2", "b", "d"}),
                new GenericRow(new object[] {"3", null, "f"}),
                new GenericRow(new object[] {"4", "a", null}),
                new GenericRow(new object[] {"5", "a", "f"}),
                new GenericRow(new object[] {"6", null, "d"}),
                new GenericRow(new object[] {"7", null, "d"}),
                new GenericRow(new object[] {"8", "b", null}),
                new GenericRow(new object[] {"9", "a", "f"}),
                new GenericRow(new object[] {"10", null, null}),
                new GenericRow(new object[] {"11", null, "f"}),
                new GenericRow(new object[] {"12", null, "d"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType()),
                    new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }


        public static void AssertSameRows(DataFrame dataFrameA, DataFrame dataFrameB, Option<ITestOutputHelper> helper)
        {
            Column[] dfAOrderedColumns = dataFrameA
                .Columns()
                .OrderByDescending(val => val)
                .Select(Column)
                .ToArray();
            Column[] dfBOrderedColumns = dataFrameB
                .Columns()
                .OrderByDescending(val => val)
                .Select(Column)
                .ToArray();

            IEnumerable<Row> dfASeq = dataFrameA.Select(dfAOrderedColumns).Collect();
            IEnumerable<Row> dfBSeq = dataFrameB.Select(dfBOrderedColumns).Collect();

            foreach (Row rowA in dfASeq)
            {
                if (helper.HasValue)
                {
                    helper.Value.WriteLine($"Computed - {rowA}");
                }
            }

            int i = 0;
            foreach (Row rowA in dfASeq)
            {
                dfBSeq.Select(row => row.Values.ToString()).ShouldContain(rowA.Values.ToString());
            }
        }

        public static DataFrame GetDFFull(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "a", "c"}),
                new GenericRow(new object[] {"2", "a", "c"}),
                new GenericRow(new object[] {"3", "a", "c"}),
                new GenericRow(new object[] {"4", "b", "d"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType()),
                    new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDFWithNRows(SparkSession sparkSession, int N)
        {
            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("c0", new StringType()),
                    new StructField("c1", new StringType()),
                    new StructField("c2", new StringType())
                });

            return sparkSession.CreateDataFrame(
                Enumerable.Range(1, N).Select(value =>
                {
                    return new GenericRow(new object[] { $"{value}", $"c1-r{value}", $"c2-r{value}" });
                }).ToList(), schema);
        }

        public static DataFrame GetDFWithNegativeNumbers(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "-1", "-1.0"}),
                new GenericRow(new object[] {"2", "-2", "-2.0"}),
                new GenericRow(new object[] {"3", "-3", "-3.0"}),
                new GenericRow(new object[] {"4", "-4", "-4.0"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType()),
                    new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDFWithUniqueColumns(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "0", "3", "1", "5", "0"}),
                new GenericRow(new object[] {"2", "0", "3", "2", "6", "0"}),
                new GenericRow(new object[] {"3", "0", "3", null, "7", "0"}),
                new GenericRow(new object[] {"4", "5", null, "3", "0", "4"}),
                new GenericRow(new object[] {"5", "6", null, "4", "0", "5"}),
                new GenericRow(new object[] {"6", "7", null, "5", "0", "6"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("unique", new StringType()),
                    new StructField("nonUnique", new StringType()),
                    new StructField("nonUniqueWithNulls", new StringType()),
                    new StructField("uniqueWithNulls", new StringType()),
                    new StructField("onlyUniqueWithOtherNonUnique", new StringType()),
                    new StructField("halfUniqueCombinedWithNonUnique", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithConditionallyUninformativeColumns(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {1, 0}),
                new GenericRow(new object[] {2, 0}),
                new GenericRow(new object[] {3, 0})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new IntegerType()), new StructField("att2", new IntegerType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithConditionallyInformativeColumns(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {1, 4}),
                new GenericRow(new object[] {2, 5}),
                new GenericRow(new object[] {3, 6})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new IntegerType()), new StructField("att2", new IntegerType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfFractionalIntegralTypes(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "1.0"}), new GenericRow(new object[] {"2", "1"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()), new StructField("att1", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfFractionalStringTypes(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "1.0"}), new GenericRow(new object[] {"2", "a"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()), new StructField("att1", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithDistinctValues(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"a", null}),
                new GenericRow(new object[] {"a", null}),
                new GenericRow(new object[] {null, "x"}),
                new GenericRow(new object[] {"b", "x"}),
                new GenericRow(new object[] {"b", "x"}),
                new GenericRow(new object[] {"c", "y"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new StringType()), new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithNumericValues(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", 1, 0, 0}),
                new GenericRow(new object[] {"2", 2, 0, 0}),
                new GenericRow(new object[] {"3", 3, 0, 0}),
                new GenericRow(new object[] {"4", 4, 5, 4}),
                new GenericRow(new object[] {"5", 5, 6, 6}),
                new GenericRow(new object[] {"6", 6, 7, 7})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new IntegerType()),
                    new StructField("att2", new IntegerType()),
                    new StructField("att3", new IntegerType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithNumericFractionalValues(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", 1.0, 0.0}),
                new GenericRow(new object[] {"2", 2.0, 0.0}),
                new GenericRow(new object[] {"3", 3.0, 0.0}),
                new GenericRow(new object[] {"4", 4.0, 0.0}),
                new GenericRow(new object[] {"5", 5.0, 0.0}),
                new GenericRow(new object[] {"6", 6.0, 0.0})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new IntegerType()),
                    new StructField("att2", new IntegerType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithVariableStringLengthValues(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {""}),
                new GenericRow(new object[] {"a"}),
                new GenericRow(new object[] {"bb"}),
                new GenericRow(new object[] {"ccc"}),
                new GenericRow(new object[] {"dddd"})
            };

            StructType schema = new StructType(
                new List<StructField> { new StructField("att1", new StringType()) });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfCompleteAndInCompleteColumns(SparkSession sparkSession)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "a", "f"}),
                new GenericRow(new object[] {"2", "b", "d"}),
                new GenericRow(new object[] {"3", "a", null}),
                new GenericRow(new object[] {"4", "a", "f"}),
                new GenericRow(new object[] {"5", "b", null}),
                new GenericRow(new object[] {"6", "a", "f"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType()),
                    new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithNestedColumn(SparkSession session)
        {
            DataFrameReader dfr = session.Read().Format("json");
            return dfr.Json("nested-test.json");
        }

        public static DataFrame GetDfWithStrongPositiveCorrelation(SparkSession session)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] { 1, 2}),
                new GenericRow(new object[] { 2, 4}),
                new GenericRow(new object[] { 3, 6}),
                new GenericRow(new object[] { 4, 8}),
                new GenericRow(new object[] { 5, 10}),
                new GenericRow(new object[] { 6, 12})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new IntegerType()),
                    new StructField("att2", new IntegerType())
                });

            return session.CreateDataFrame(elements, schema);
        }


        public static DataFrame GetDfWithStrongNegativeCorrelation(SparkSession session)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] { 1, 12}),
                new GenericRow(new object[] { 2, 10}),
                new GenericRow(new object[] { 3, 8}),
                new GenericRow(new object[] { 4, 6}),
                new GenericRow(new object[] { 5, 4}),
                new GenericRow(new object[] { 6, 2})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new IntegerType()),
                    new StructField("att2", new IntegerType())
                });

            return session.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithLowCorrelation(SparkSession session)
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] { 12, 8}),
                new GenericRow(new object[] { 10, 12}),
                new GenericRow(new object[] { 8, 1}),
                new GenericRow(new object[] { 6, 30}),
                new GenericRow(new object[] { 4, 9}),
                new GenericRow(new object[] { 2, 7})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new IntegerType()),
                    new StructField("att2", new IntegerType())
                });

            return session.CreateDataFrame(elements, schema);
        }

        public static DataFrame DataFrameWithColumn(string name, DataType sparkDt, SparkSession session,
            GenericRow[] values) =>
            session.CreateDataFrame(values,
                new StructType(new[] { new StructField(name, sparkDt) })).ToDF(name);
    }
}
