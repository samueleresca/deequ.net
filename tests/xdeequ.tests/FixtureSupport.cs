using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace xdeequ.tests
{
    public static class FixtureSupport
    {
        public static DataFrame GetDFMissing(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
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

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType()),
                    new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDFFull(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "a", "c"}),
                new GenericRow(new object[] {"2", "a", "c"}),
                new GenericRow(new object[] {"3", "a", "c"}),
                new GenericRow(new object[] {"4", "b", "d"})
            };

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType()),
                    new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDFWithNegativeNumbers(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "-1", "-1.0"}),
                new GenericRow(new object[] {"2", "-2", "-2.0"}),
                new GenericRow(new object[] {"3", "-3", "-3.0"}),
                new GenericRow(new object[] {"4", "-4", "-4.0"})
            };

            var schema = new StructType(
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
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "0", "3", "1", "5", "0"}),
                new GenericRow(new object[] {"2", "0", "3", "2", "6", "0"}),
                new GenericRow(new object[] {"3", "0", "3", null, "7", "0"}),
                new GenericRow(new object[] {"4", "5", null, "3", "0", "4"}),
                new GenericRow(new object[] {"5", "6", null, "4", "0", "5"}),
                new GenericRow(new object[] {"6", "7", null, "5", "0", "6"})
            };

            var schema = new StructType(
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
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {1, 0}),
                new GenericRow(new object[] {2, 0}),
                new GenericRow(new object[] {3, 0})
            };

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new IntegerType()),
                    new StructField("att2", new IntegerType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }
        
        public static DataFrame GetDfWithConditionallyInformativeColumns(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {1, 4}),
                new GenericRow(new object[] {2, 5}),
                new GenericRow(new object[] {3, 6})
            };

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new IntegerType()),
                    new StructField("att2", new IntegerType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfFractionalIntegralTypes(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "1.0"}),
                new GenericRow(new object[] {"2", "1"})
            };

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfFractionalStringTypes(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "1.0"}),
                new GenericRow(new object[] {"2", "a"})
            };

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("item", new StringType()),
                    new StructField("att1", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithDistinctValues(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"a", null}),
                new GenericRow(new object[] {"a", null}),
                new GenericRow(new object[] {null, "x"}),
                new GenericRow(new object[] {"b", "x"}),
                new GenericRow(new object[] {"b", "x"}),
                new GenericRow(new object[] {"c", "y"})
            };

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new StringType()),
                    new StructField("att2", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfWithNumericValues(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", 1, 0, 0}),
                new GenericRow(new object[] {"2", 2, 0, 0}),
                new GenericRow(new object[] {"3", 3, 0, 0}),
                new GenericRow(new object[] {"4", 4, 5, 4}),
                new GenericRow(new object[] {"5", 5, 6, 6}),
                new GenericRow(new object[] {"6", 6, 7, 7})
            };

            var schema = new StructType(
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
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", 1.0, 0.0}),
                new GenericRow(new object[] {"2", 2.0, 0.0}),
                new GenericRow(new object[] {"3", 3.0, 0.0}),
                new GenericRow(new object[] {"4", 4.0, 0.0}),
                new GenericRow(new object[] {"5", 5.0, 0.0}),
                new GenericRow(new object[] {"6", 6.0, 0.0})
            };

            var schema = new StructType(
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
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {""}),
                new GenericRow(new object[] {"a"}),
                new GenericRow(new object[] {"bb"}),
                new GenericRow(new object[] {"ccc"}),
                new GenericRow(new object[] {"dddd"})
            };

            var schema = new StructType(
                new List<StructField>
                {
                    new StructField("att1", new StringType())
                });

            return sparkSession.CreateDataFrame(elements, schema);
        }

        public static DataFrame GetDfCompleteAndInCompleteColumns(SparkSession sparkSession)
        {
            var elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1", "a", "f"}),
                new GenericRow(new object[] {"2", "b", "d"}),
                new GenericRow(new object[] {"3", "a", null}),
                new GenericRow(new object[] {"4", "a", "f"}),
                new GenericRow(new object[] {"5", "b", null}),
                new GenericRow(new object[] {"6", "a", "f"})
            };

            var schema = new StructType(
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
            var dfr = session.Read().Format("json");
            return dfr.Json("nested-test.json");
        }

        public static DataFrame DataFrameWithColumn(string name, DataType sparkDt, SparkSession session,
            GenericRow[] values)
        {
            return session.CreateDataFrame(values,
                new StructType(new[] { new StructField(name, sparkDt) })).ToDF(name);
        }
    }
}