using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Repository;
using xdeequ.Util;
using Xunit;
using Xunit.Abstractions;

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class AnalysisResultTest
    {
        public AnalysisResultTest(SparkFixture fixture) => _session = fixture.Spark;

        private static readonly long DATE_ONE = new DateTime(2021, 10, 14).ToBinary();

        private static readonly KeyValuePair<string, string>[] REGION_EU =
        {
            new KeyValuePair<string, string>("Region", "EU")
        };

        private static readonly KeyValuePair<string, string>[] REGION_EU_INVALID =
        {
            new KeyValuePair<string, string>("Re%%^gion!/", "EU")
        };

        private static KeyValuePair<string, string>[] DUPLICATE_COLUMN_NAME =
        {
            new KeyValuePair<string, string>("name", "EU")
        };

        private static KeyValuePair<string, string>[] MULTIPLE_TAGS =
        {
            new KeyValuePair<string, string>("Region", "EU"),
            new KeyValuePair<string, string>("Data Set Name", "Some")
        };

        private readonly SparkSession _session;

        private static void Evaluate(SparkSession session, Action<AnalyzerContext> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            AnalyzerContext results = CreateAnalysis().Run(data, Option<IStateLoader>.None,
                Option<IStatePersister>.None,
                new StorageLevel());

            func(results);
        }

        private static Analysis CreateAnalysis() =>
            new Analysis()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] { "item" }, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att1"))
                .AddAnalyzer(Initializers.Uniqueness(new[] { "att1", "att2" }));


        private static void AssertSameRows(string jsonA, string jsonB)
        {
            SimpleMetricOutput[] resultA =
                JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonA, SerdeExt.GetDefaultOptions());
            SimpleMetricOutput[] resultB =
                JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonB, SerdeExt.GetDefaultOptions());
            int i = 0;

            foreach (SimpleMetricOutput rowA in resultA)
            {
                SimpleMetricOutput rowB = resultB.Skip(i).First();

                rowA.Entity.ShouldBe(rowB.Entity);
                rowA.Instance.ShouldBe(rowB.Instance);
                rowA.Name.ShouldBe(rowB.Name);
                rowA.Value.ShouldBe(rowB.Value);

                i++;
            }
        }

        [Fact]
        public void correctly_return_a_DataFrame_that_is_formatted_as_expected() =>
            Evaluate(_session, context =>
            {
                ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));

                DataFrame analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"})
                };

                StructType schema = new StructType(
                    new List<StructField>
                    {
                        new StructField("entity", new StringType()),
                        new StructField("instance", new StringType()),
                        new StructField("name", new StringType()),
                        new StructField("value", new DoubleType()),
                        new StructField("dataset_date", new LongType()),
                        new StructField("region", new StringType())
                    });

                DataFrame df = _session.CreateDataFrame(elements, schema);

                FixtureSupport.AssertSameRows(analysisResultsAsDataFrame, df, Option<ITestOutputHelper>.None);
            });

        [Fact]
        public void correctly_return_a_JSON_that_is_formatted_as_expected() =>
            Evaluate(_session, context =>
            {
                ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));

                string analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());


                string expected =
                    "[{\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}]";
                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString());

                AssertSameRows(analysisResultsAsDataFrame, expected);
            });

        [Fact]
        public void only_include_requested_metrics_in_returned_DataFrame() =>
            Evaluate(_session, context =>
            {
                ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));
                IAnalyzer<DoubleMetric>[] metricsForAnalyzers =
                {
                    Initializers.Completeness("att1"), Initializers.Uniqueness(new[] {"att1", "att2"})
                };

                DataFrame analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsDataFrame(_session, metricsForAnalyzers,
                        Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"})
                };

                StructType schema = new StructType(
                    new List<StructField>
                    {
                        new StructField("entity", new StringType()),
                        new StructField("instance", new StringType()),
                        new StructField("name", new StringType()),
                        new StructField("value", new DoubleType()),
                        new StructField("dataset_date", new LongType()),
                        new StructField("region", new StringType())
                    });

                DataFrame df = _session.CreateDataFrame(elements, schema);


                FixtureSupport.AssertSameRows(analysisResultsAsDataFrame, df, Option<ITestOutputHelper>.None);
            });

        [Fact]
        public void only_include_requested_metrics_in_returned_JSON() =>
            Evaluate(_session, context =>
            {
                ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));
                IAnalyzer<DoubleMetric>[] metricsForAnalyzers =
                {
                    Initializers.Completeness("att1"), Initializers.Uniqueness(new[] {"att1", "att2"})
                };

                string analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsJson(metricsForAnalyzers,
                        Enumerable.Empty<string>());


                string expected =
                    "[{\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}]";

                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString());


                AssertSameRows(analysisResultsAsDataFrame, expected);
            });

        [Fact]
        public void return_empty_DataFrame_if_AnalyzerContext_contains_no_entries()
        {
            DataFrame data = FixtureSupport.GetDFFull(_session);
            ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_INVALID));

            AnalyzerContext results = new Analysis().Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None,
                new StorageLevel());
            DataFrame analysisResultsAsDataFrame = new AnalysisResult(resultKey, results)
                .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>(),
                    Enumerable.Empty<string>());

            List<GenericRow> elements = new List<GenericRow>();

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("entity", new StringType()),
                    new StructField("instance", new StringType()),
                    new StructField("name", new StringType()),
                    new StructField("value", new DoubleType()),
                    new StructField("dataset_date", new LongType()),
                    new StructField("region", new StringType())
                });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            FixtureSupport.AssertSameRows(analysisResultsAsDataFrame, df, Option<ITestOutputHelper>.None);
        }

        [Fact]
        public void return_empty_JSON_if_AnalyzerContext_contains_no_entries()
        {
            DataFrame data = FixtureSupport.GetDFFull(_session);
            ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_INVALID));

            AnalyzerContext results = new Analysis().Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None,
                new StorageLevel());
            string analysisResultsAsDataFrame = new AnalysisResult(resultKey, results)
                .GetSuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>(),
                    Enumerable.Empty<string>());

            string expected = "[]";

            AssertSameRows(analysisResultsAsDataFrame, expected);
        }


        [Fact]
        public void turn_tagNames_into_valid_columnNames_in_returned_DataFrame() =>
            Evaluate(_session, context =>
            {
                ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_INVALID));

                DataFrame analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"})
                };

                StructType schema = new StructType(
                    new List<StructField>
                    {
                        new StructField("entity", new StringType()),
                        new StructField("instance", new StringType()),
                        new StructField("name", new StringType()),
                        new StructField("value", new DoubleType()),
                        new StructField("dataset_date", new LongType()),
                        new StructField("region", new StringType())
                    });

                DataFrame df = _session.CreateDataFrame(elements, schema);

                FixtureSupport.AssertSameRows(analysisResultsAsDataFrame, df, Option<ITestOutputHelper>.None);
            });

        [Fact]
        public void turn_tagNames_into_valid_columnNames_in_returned_JSON() =>
            Evaluate(_session, context =>
            {
                ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_INVALID));

                string analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());

                string expected =
                    "[{\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}]";

                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString());


                AssertSameRows(analysisResultsAsDataFrame, expected);
            });
    }
}
