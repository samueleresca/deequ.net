using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Checks;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using Xunit.Abstractions;

namespace xdeequ.tests
{
    [Collection("Spark instance")]
    public class ValidationResultTests
    {
        public ValidationResultTests(SparkFixture fixture) => _session = fixture.Spark;
        public SparkSession _session;

        private static void Evaluate(SparkSession session, Action<VerificationResult> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            IEnumerable<IAnalyzer<IMetric>> analyzers = GetAnalyzers();
            IEnumerable<Check> checks = GetChecks();

            VerificationResult results = new VerificationSuite()
                .OnData(data)
                .AddRequiredAnalyzers(analyzers)
                .AddChecks(checks)
                .Run();

            func(results);
        }

        private static IEnumerable<Check> GetChecks()
        {
            CheckWithLastConstraintFilterable checkToSucceed = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None);

            CheckWithLastConstraintFilterable checkToErrorOut = new Check(CheckLevel.Error, "group-2-E")
                .HasSize(_ => _ > 5, "Should be greater than 5!")
                .HasCompleteness("att2", _ => _ == 1.0, "Should equal 1!");

            CheckWithLastConstraintFilterable checkToWarn = new Check(CheckLevel.Warning, "group-2-W")
                .HasDistinctness(new[] { "item" }, _ => _ < 0.8, "Should be smaller than 0.8!");


            return new[] { checkToSucceed, checkToErrorOut, checkToWarn };
        }

        private static IEnumerable<IAnalyzer<IMetric>> GetAnalyzers() =>
            new[] { Initializers.Uniqueness(new[] { "att1", "att2" }) };

        private static void AssertSameRows(string jsonA, string jsonB)
        {
            SimpleMetricOutput[] resultA =
                JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonA, SerdeExt.GetDefaultOptions());
            SimpleMetricOutput[] resultB =
                JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonB, SerdeExt.GetDefaultOptions());

            foreach (SimpleMetricOutput rowA in resultA)
            {
                resultB.Any(x => rowA.Entity == x.Entity
                                 && rowA.Instance == x.Instance
                                 && rowA.Name == x.Name
                                 && rowA.Value == x.Value).ShouldBeTrue();
            }
        }


        private static void AssertSameRowsDictionary(string jsonA, string jsonB)
        {
            IEnumerable<Dictionary<string, string>> resultA =
                JsonSerializer.Deserialize<IEnumerable<Dictionary<string, string>>>(jsonA,
                    SerdeExt.GetDefaultOptions());
            IEnumerable<Dictionary<string, string>> resultB =
                JsonSerializer.Deserialize<IEnumerable<Dictionary<string, string>>>(jsonB,
                    SerdeExt.GetDefaultOptions());

            foreach (Dictionary<string, string> value in resultA)
            {
                resultB.Any(x => x.OrderBy(y => y.Key)
                    .SequenceEqual(value.OrderBy(y => y.Key))).ShouldBeTrue();
            }
        }

        [Fact]
        public void getCheckResults_correctly_return_a_DataFrame_that_is_formatted_as_expected() =>
            Evaluate(_session, results =>
            {
                DataFrame successMetricsAsDataFrame = new VerificationResult(results).CheckResultsAsDataFrame();

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[]
                    {
                        "group-1", "Error", "Success", "CompletenessConstraint(Completeness(att1,None))", "Success",
                        ""
                    }),
                    new GenericRow(new object[]
                    {
                        "group-2-E", "Error", "Error", "SizeConstraint(Size(None))", "Failure",
                        "Value: 4 does not meet the constraint requirement!Should be greater than 5!"
                    }),
                    new GenericRow(new object[]
                    {
                        "group-2-E", "Error", "Error", "CompletenessConstraint(Completeness(att2,None))", "Success",
                        ""
                    }),
                    new GenericRow(new object[]
                    {
                        "group-2-W", "Warning", "Warning", "DistinctnessConstraint(Distinctness(List(item),None))",
                        "Failure", "Value: 1 does not meet the constraint requirement!" +
                                   "Should be smaller than 0.8!"
                    })
                };

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

                DataFrame df = _session.CreateDataFrame(elements, schema);

                FixtureSupport.AssertSameRows(successMetricsAsDataFrame, df, Option<ITestOutputHelper>.None);
            });


        [Fact]
        public void getCheckResults_correctly_return_Json_that_is_formatted_as_expected() =>
            Evaluate(_session, results =>
            {
                string successMetricsAsDataFrame =
                    new VerificationResult(results).CheckResultAsJson(results, Enumerable.Empty<Check>());

                string expected =
                    "[{\"check\":\"group-1\",\"check_level\":\"Error\",\"check_status\":\"Success\",\"constraint\":\"CompletenessConstraint(Completeness(att1,None))\",\"constraint_status\":\"Success\",\"constraint_message\":\"\"}," +
                    "{\"check\":\"group-2-E\",\"check_level\":\"Error\",\"check_status\":\"Error\",\"constraint\":\"SizeConstraint(Size(None))\", \"constraint_status\":\"Failure\",\"constraint_message\":\"Value: 4 does not meet the constraint requirement!Should be greater than 5!\"}," +
                    "{\"check\":\"group-2-E\",\"check_level\":\"Error\",\"check_status\":\"Error\",\"constraint\":\"CompletenessConstraint(Completeness(att2,None))\",\"constraint_status\":\"Success\",\"constraint_message\":\"\"}," +
                    "{\"check\":\"group-2-W\",\"check_level\":\"Warning\",\"check_status\":\"Warning\",\"constraint\":\"DistinctnessConstraint(Distinctness(List(item),None))\",\"constraint_status\":\"Failure\",\"constraint_message\":\"Value: 1 does not meet the constraint requirement!Should be smaller than 0.8!\"}]";

                AssertSameRowsDictionary(successMetricsAsDataFrame, expected);
            });


        [Fact]
        public void getSuccessMetric_correctly_return_a_DataFrame_that_is_formatted_as_expected() =>
            Evaluate(_session, results =>
            {
                DataFrame successMetricsAsDataFrame = results
                    .SuccessMetricsAsDataFrame(_session,
                        Enumerable.Empty<IAnalyzer<IMetric>>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0}),
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0}),
                    new GenericRow(new object[] {"Column", "att2", "Completeness", 1.0}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0})
                };

                StructType schema = new StructType(
                    new List<StructField>
                    {
                        new StructField("entity", new StringType()),
                        new StructField("instance", new StringType()),
                        new StructField("name", new StringType()),
                        new StructField("value", new DoubleType())
                    });

                DataFrame df = _session.CreateDataFrame(elements, schema);

                FixtureSupport.AssertSameRows(successMetricsAsDataFrame, df, Option<ITestOutputHelper>.None);
            });


        [Fact]
        public void getSuccessMetric_correctly_return_Json_that_is_formatted_as_expected() =>
            Evaluate(_session, results =>
            {
                string successMetricsAsJson = results
                    .SuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>());

                string expected =
                    "[{\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0}," +
                    " {\"entity\": \"Column\", \"instance\":\"att2\",\"name\":\"Completeness\",\"value\":1.0}," +
                    " {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0}," +
                    " {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25}," +
                    " {\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0}]";

                AssertSameRows(successMetricsAsJson, expected);
            });

        [Fact]
        public void getSuccessMetric_only_include_requested_metrics_in_returned_Json() =>
            Evaluate(_session, results =>
            {
                IAnalyzer<IMetric>[] metricsForAnalyzers =
                {
                    new Completeness("att1"), new Uniqueness(new[] {"att1", "att2"})
                };
                string successMetricsAsJson = results
                    .SuccessMetricsAsJson(metricsForAnalyzers);

                string expected =
                    "[{\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0}," +
                    "{\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\",\"name\":\"Uniqueness\",\"value\":0.25}]";

                AssertSameRows(successMetricsAsJson, expected);
            });


        [Fact]
        public void getSuccessMetric_only_include_specific_metrics_in_returned_DataFrame_if_requested() =>
            Evaluate(_session, results =>
            {
                IAnalyzer<DoubleMetric>[] metricsForAnalyzers =
                {
                    Initializers.Completeness("att1"), Initializers.Uniqueness(new[] {"att1", "att2"})
                };

                DataFrame successMetricsAsDataFrame = results
                    .SuccessMetricsAsDataFrame(_session,
                        metricsForAnalyzers);

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25})
                };

                StructType schema = new StructType(
                    new List<StructField>
                    {
                        new StructField("entity", new StringType()),
                        new StructField("instance", new StringType()),
                        new StructField("name", new StringType()),
                        new StructField("value", new DoubleType())
                    });

                DataFrame df = _session.CreateDataFrame(elements, schema);

                FixtureSupport.AssertSameRows(successMetricsAsDataFrame, df, Option<ITestOutputHelper>.None);
            });
    }
}
