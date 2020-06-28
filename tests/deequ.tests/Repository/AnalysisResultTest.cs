using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
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

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class AnalysisResultTest
    {

        private static long DATE_ONE = new DateTime(2021, 10, 14).ToBinary();
        private static KeyValuePair<string, string>[] REGION_EU = { new KeyValuePair<string, string>("Region", "EU") };

        private static KeyValuePair<string, string>[] REGION_EU_INVALID =
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

        public AnalysisResultTest(SparkFixture fixture) => _session = fixture.Spark;

        [Fact]
        public void correctly_return_a_DataFrame_that_is_formatted_as_expected()
        {
            Evaluate(_session, context =>
            {
                var resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));

                var analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());


                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"}),
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

                var df = _session.CreateDataFrame(elements, schema);

                AssertSameRows(analysisResultsAsDataFrame, df);

            });
        }

        [Fact]
        public void correctly_return_a_JSON_that_is_formatted_as_expected()
        {
            Evaluate(_session, context =>
            {
                var resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));

                var analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsJson(_session, Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());


                var expected = "[{\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}]";
                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString());

                AssertSameRows(analysisResultsAsDataFrame, expected);

            });
        }

        [Fact]
        public void only_include_requested_metrics_in_returned_DataFrame()
        {
            Evaluate(_session, context =>
            {
                var resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));
                var metricsForAnalyzers = new IAnalyzer<DoubleMetric>[]
                {
                    Initializers.Completeness("att1"),
                    Initializers.Uniqueness(new[] {"att1", "att2"})
                };

                var analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsDataFrame(_session, metricsForAnalyzers,
                        Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"}),
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

                var df = _session.CreateDataFrame(elements, schema);


                AssertSameRows(analysisResultsAsDataFrame, df);

            });
        }

        [Fact]
        public void only_include_requested_metrics_in_returned_JSON()
        {
            Evaluate(_session, context =>
            {
                var resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));
                var metricsForAnalyzers = new IAnalyzer<DoubleMetric>[]
                {
                    Initializers.Completeness("att1"),
                    Initializers.Uniqueness(new[] {"att1", "att2"})
                };

                var analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsJson(_session, metricsForAnalyzers,
                        Enumerable.Empty<string>());


                var expected =
                    "[{\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}]";

                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString());


                AssertSameRows(analysisResultsAsDataFrame, expected);

            });
        }


        [Fact]
        public void turn_tagNames_into_valid_columnNames_in_returned_DataFrame()
        {
            Evaluate(_session, context =>
            {
                var resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_INVALID));

                var analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"}),
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

                var df = _session.CreateDataFrame(elements, schema);

                AssertSameRows(analysisResultsAsDataFrame, df);

            });
        }


        [Fact]
        public void turn_tagNames_into_valid_columnNames_in_returned_JSON()
        {
            Evaluate(_session, context =>
            {
                var resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_INVALID));

                var analysisResultsAsDataFrame = new AnalysisResult(resultKey, context)
                    .GetSuccessMetricsAsJson(_session, Enumerable.Empty<IAnalyzer<IMetric>>(),
                        Enumerable.Empty<string>());

                var expected =
                    "[{\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}]";

                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString());


                AssertSameRows(analysisResultsAsDataFrame, expected);

            });
        }

        private static void Evaluate(SparkSession session, Action<AnalyzerContext> func)
        {

            var data = FixtureSupport.GetDFFull(session);

            var results = CreateAnalysis().Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None,
                new StorageLevel());

            func(results);
        }

        private static Analysis CreateAnalysis()
        {
            return new Analysis()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] { "item" }, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att1"))
                .AddAnalyzer(Initializers.Uniqueness(new[] { "att1", "att2" }));
        }

        private static void AssertSameRows(DataFrame dataFrameA, DataFrame dataFrameB)
        {

            var dfASeq = dataFrameA.Collect();
            var dfBSeq = dataFrameB.Collect();

            var i = 0;
            foreach (var rowA in dfASeq)
            {
                var rowB = dfBSeq.Skip(i).First();

                rowA[0].ShouldBe(rowB[0]);
                rowA[1].ShouldBe(rowB[1]);
                rowA[2].ShouldBe(rowB[2]);
                rowA[3].ShouldBe(rowB[3]);
                rowA[4].ShouldBe(rowB[4]);
                rowA[5].ShouldBe(rowB[5]);

                i++;
            }
        }

        private static void AssertSameRows(string jsonA, string jsonB)
        {
            var resultA = JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonA, SerdeExt.GetDefaultOptions());
            var resultB = JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonB, SerdeExt.GetDefaultOptions());
            var i = 0;

            foreach (var rowA in resultA)
            {
                var rowB = resultB.Skip(i).First();

                rowA.Entity.ShouldBe(rowB.Entity);
                rowA.Instance.ShouldBe(rowB.Instance);
                rowA.Name.ShouldBe(rowB.Name);
                rowA.Value.ShouldBe(rowB.Value);

                i++;
            }
        }
    }
}
