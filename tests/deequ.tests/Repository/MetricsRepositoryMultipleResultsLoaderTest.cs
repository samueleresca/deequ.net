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
using xdeequ.Repository.InMemory;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class MetricsRepositoryMultipleResultsLoaderTest
    {
        private static readonly long DATE_ONE = new DateTime(2021, 10, 14).ToBinary();
        private static readonly long DATE_TWO = new DateTime(2021, 10, 15).ToBinary();

        private static readonly KeyValuePair<string, string>[] REGION_EU =
        {
            new KeyValuePair<string, string>("Region", "EU")
        };

        private static readonly KeyValuePair<string, string>[] REGION_NA =
        {
            new KeyValuePair<string, string>("Region", "NA")
        };

        private static readonly KeyValuePair<string, string>[] REGION_EU_AND_DATASET_NAME =
        {
            new KeyValuePair<string, string>("Region", "EU"),
            new KeyValuePair<string, string>("dataset_name", "Some")
        };

        private static readonly KeyValuePair<string, string>[] REGION_NA_AND_DATASET_VERSION =
        {
            new KeyValuePair<string, string>("Region", "EU"),
            new KeyValuePair<string, string>("dataset_version", "2.0")
        };

        private readonly SparkSession _session;

        public MetricsRepositoryMultipleResultsLoaderTest(SparkFixture fixture) => _session = fixture.Spark;


        [Fact]
        public void correctly_return_a_DataFrame_of_multiple_AnalysisResults_that_is_formatted_as_expected()
        {
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);

                DataFrame analysisResultsAsDataFrame = repository.Load()
                    .After(DATE_ONE)
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_TWO, "NA"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_TWO, "NA"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"})
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

                AssertSameRows(analysisResultsAsDataFrame, df);
            });
        }

        [Fact]
        public void correctly_return_a_JSON_of_multiple_AnalysisResults_that_is_formatted_as_expected()
        {
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);

                var analysisResultsAsDataFrame = repository.Load()
                    .GetSuccessMetricsAsJson(_session, Enumerable.Empty<string>());

                var expected =
                    "[{\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE},  {\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"NA\", \"dataset_date\":$DATE_TWO}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"NA\", \"dataset_date\":$DATE_TWO}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"NA\", \"dataset_date\":$DATE_TWO}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\",\"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"NA\", \"dataset_date\":$DATE_TWO}]";
                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString()).Replace("$DATE_TWO", DATE_TWO.ToString());

                AssertSameRows(analysisResultsAsDataFrame, expected);
            });
        }

        [Fact]
        public void support_saving_data_with_different_tags_and_returning_DataFrame_with_them()
        {
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_AND_DATASET_NAME)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA_AND_DATASET_VERSION)), context);

                DataFrame analysisResultsAsDataFrame = repository.Load()
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<string>());


                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "EU", null, "Some"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU", null, "Some"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "EU", null, "Some"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU", null, "Some"}),
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_TWO, "NA", "2.0", null}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_TWO, "NA", "2.0", null}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_TWO, "NA", "2.0", null}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA", "2.0", null})
                };

                StructType schema = new StructType(
                    new List<StructField>
                    {
                        new StructField("entity", new StringType()),
                        new StructField("instance", new StringType()),
                        new StructField("name", new StringType()),
                        new StructField("value", new DoubleType()),
                        new StructField("dataset_date", new LongType()),
                        new StructField("region", new StringType()),
                        new StructField("dataset_version",  new StringType()),
                        new StructField("dataset_name", new StringType()),
                    });

                DataFrame df = _session.CreateDataFrame(elements, schema);

                AssertSameRows(analysisResultsAsDataFrame, df);
            });

        }


        [Fact]
        public void support_saving_data_with_different_tags_and_returning_JSON_with_them()
        {
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU_AND_DATASET_NAME)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA_AND_DATASET_VERSION)), context);

                var analysisResultsAsDataFrame = repository.Load()
                    .GetSuccessMetricsAsJson(_session, Enumerable.Empty<string>());

                var expected =
                    "[{\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE, \"dataset_name\":\"Some\", \"dataset_version\":null}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE, \"dataset_name\":\"Some\", \"dataset_version\":null}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"EU\", \"dataset_date\":$DATE_ONE, \"dataset_name\":\"Some\", \"dataset_version\":null}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"EU\", \"dataset_date\":$DATE_ONE, \"dataset_name\":\"Some\", \"dataset_version\":null},  {\"entity\":\"Dataset\",\"instance\":\"*\",\"name\":\"Size\",\"value\":4.0, \"region\":\"NA\", \"dataset_date\":$DATE_TWO, \"dataset_name\":null, \"dataset_version\":\"2.0\"}, {\"entity\":\"Column\",\"instance\":\"att1\",\"name\":\"Completeness\",\"value\":1.0, \"region\":\"NA\", \"dataset_date\":$DATE_TWO, \"dataset_name\":null, \"dataset_version\":\"2.0\"}, {\"entity\":\"Column\",\"instance\":\"item\",\"name\":\"Distinctness\",\"value\":1.0, \"region\":\"NA\", \"dataset_date\":$DATE_TWO, \"dataset_name\":null, \"dataset_version\":\"2.0\"}, {\"entity\":\"Multicolumn\",\"instance\":\"att1,att2\", \"name\":\"Uniqueness\",\"value\":0.25, \"region\":\"NA\", \"dataset_date\":$DATE_TWO, \"dataset_name\":null, \"dataset_version\":\"2.0\"}]";
                expected = expected.Replace("$DATE_ONE", DATE_ONE.ToString()).Replace("$DATE_TWO", DATE_TWO.ToString());

                AssertSameRows(analysisResultsAsDataFrame, expected);
            });
        }

        private static void Evaluate(SparkSession session, Action<AnalyzerContext, IMetricsRepository> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            AnalyzerContext results = CreateAnalysis()
                .Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None, new StorageLevel());

            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();

            func(results, repository);
        }

        [Fact]
        public void return_empty_Seq_if_load_parameters_too_restrictive() =>
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);

                var analysisResultsAsDataFrame = repository.Load()
                    .After(DATE_TWO)
                    .Before(DATE_ONE)
                    .GetSuccessMetricsAsJson(_session, Enumerable.Empty<string>());

                var expected = "[]";

                AssertSameRows(analysisResultsAsDataFrame, expected);
            });

        [Fact]
        public void only_include_specifics_metrics_in_loaded_AnalysisResults_if_requested() =>
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);

                DataFrame analysisResultsAsDataFrame = repository.Load()
                    .After(DATE_TWO)
                    .ForAnalyzers(new List<IAnalyzer<IMetric>>
                    {
                        Initializers.Completeness("att1"), Initializers.Uniqueness(new[] {"att1", "att2"})
                    })
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "NA"}),
                    new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "NA"})
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

                AssertSameRows(analysisResultsAsDataFrame, df);
            });



        private static void AssertSameRows(DataFrame dataFrameA, DataFrame dataFrameB)
        {
            IEnumerable<Row> dfASeq = dataFrameA.Collect();
            IEnumerable<Row> dfBSeq = dataFrameB.Collect();

            int i = 0;
            foreach (Row rowA in dfASeq)
            {
                Row rowB = dfBSeq.Skip(i).First();

                rowA[0].ShouldBe(rowB[0]);
                rowA[1].ShouldBe(rowB[1]);
                rowA[2].ShouldBe(rowB[2]);
                rowA[3].ShouldBe(rowB[3]);

                i++;
            }
        }

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

        private static Analysis CreateAnalysis() =>
            new Analysis()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] { "item" }, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att1"))
                .AddAnalyzer(Initializers.Uniqueness(new[] { "att1", "att2" }));

        private static IMetricsRepository CreateRepository() => new InMemoryMetricsRepository();



    }
}
