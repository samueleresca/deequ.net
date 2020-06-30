using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Metrics;
using xdeequ.Repository;
using xdeequ.Repository.InMemory;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Repository.Memory
{
    [Collection("Spark instance")]
    public class InMemoryMetricsRepositoryTests
    {
        public InMemoryMetricsRepositoryTests(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;
        private static readonly long DATE_ONE = new DateTime(2021, 10, 14).ToBinary();
        private static readonly long DATE_TWO = new DateTime(2021, 10, 15).ToBinary();
        private static readonly long DATE_THREE = new DateTime(2021, 10, 16).ToBinary();

        private static readonly KeyValuePair<string, string>[] REGION_EU =
        {
            new KeyValuePair<string, string>("Region", "EU")
        };

        private static readonly KeyValuePair<string, string>[] REGION_NA =
        {
            new KeyValuePair<string, string>("Region", "EU")
        };

        private static void Evaluate(SparkSession session, Action<AnalyzerContext, IMetricsRepository> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            AnalyzerContext results = CreateAnalysis().Run(data, Option<IStateLoader>.None,
                Option<IStatePersister>.None,
                new StorageLevel());

            IMetricsRepository repository = CreateRepository();
            func(results, repository);
        }

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

        private static Analysis CreateAnalysis() =>
            new Analysis()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] {"item"}, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att1"))
                .AddAnalyzer(Initializers.Uniqueness(new[] {"att1", "att2"}));

        private static IMetricsRepository CreateRepository() => new InMemoryMetricsRepository();

        [Fact]
        public void include_no_metrics_in_loaded_AnalysisResults_if_requested() =>
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);

                DataFrame analysisResultsAsDataFrame = repository.Load()
                    .After(DATE_ONE)
                    .ForAnalyzers(Enumerable.Empty<IAnalyzer<IMetric>>())
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<string>());

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

                AssertSameRows(analysisResultsAsDataFrame, df);
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

        [Fact]
        public void only_load_AnalysisResults_with_a_specific_tag() =>
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);

                DataFrame analysisResultsAsDataFrame = repository.Load()
                    .After(DATE_TWO)
                    .WithTagValues(new Dictionary<string, string>(REGION_EU))
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<string>());

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

                AssertSameRows(analysisResultsAsDataFrame, df);
            });

        [Fact]
        public void only_load_AnalysisResults_with_a_specific_time_frame_if_requested() =>
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);
                repository.Save(new ResultKey(DATE_THREE, new Dictionary<string, string>(REGION_NA)), context);

                DataFrame analysisResultsAsDataFrame = repository.Load()
                    .After(DATE_TWO)
                    .Before(DATE_TWO)
                    .GetSuccessMetricsAsDataFrame(_session, Enumerable.Empty<string>());

                List<GenericRow> elements = new List<GenericRow>
                {
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "NA"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "NA"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "NA"}),
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

        [Fact]
        public void return_empty_Seq_if_load_parameters_too_restrictive() =>
            Evaluate(_session, (context, repository) =>
            {
                repository.Save(new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU)), context);
                repository.Save(new ResultKey(DATE_TWO, new Dictionary<string, string>(REGION_NA)), context);

                IEnumerable<AnalysisResult> analysisResultsAsDataFrame = repository.Load()
                    .After(DATE_TWO)
                    .Before(DATE_ONE)
                    .Get();

                analysisResultsAsDataFrame.ShouldBeEmpty();
            });


        [Fact]
        public void save_and_retrieve_AnalyzerContexts() =>
            Evaluate(_session, (context, repository) =>
            {
                ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));

                repository.Save(resultKey, context);

                AnalyzerContext loadResults = repository.LoadByKey(resultKey).Value;

                DataFrame loadedResultsAsDataFrame =
                    loadResults.SuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>());

                DataFrame resultAsDataFrame =
                    context.SuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>());

                AssertSameRows(loadedResultsAsDataFrame, resultAsDataFrame);

                loadResults
                    .SuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>())
                    .ShouldBe(context
                        .SuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>()));
            });

        [Fact]
        public void save_and_retrieve_AnalyzerResults() =>
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
                    new GenericRow(new object[] {"Dataset", "*", "Size", 4.0, DATE_ONE, "NA"}),
                    new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0, DATE_ONE, "NA"}),
                    new GenericRow(new object[] {"Column", "item", "Distinctness", 1.0, DATE_ONE, "NA"}),
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

        [Fact]
        public void save_should_ignore_failed_result_metrics_when_saving()
        {
            Dictionary<IAnalyzer<IMetric>, IMetric> metrics = new Dictionary<IAnalyzer<IMetric>, IMetric>
            {
                {
                    Initializers.Size(Option<string>.None),
                    new DoubleMetric(Entity.Column, "Size", "*", Try<double>.From(() => 5.0))
                },
                {
                    Initializers.Completeness("ColumnA"), new DoubleMetric(Entity.Column, "Completeness", "ColumnA",
                        Try<double>.From(() => throw new Exception("error")))
                }
            };

            AnalyzerContext resultsWithMixedValues = new AnalyzerContext(metrics);
            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> successMetrics =
                resultsWithMixedValues.MetricMap.Where(
                    x =>
                    {
                        DoubleMetric dm = x.Value as DoubleMetric;
                        return dm.Value.IsSuccess;
                    });

            AnalyzerContext resultsWithSuccessfulValues =
                new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(successMetrics));

            IMetricsRepository repository = CreateRepository();

            ResultKey resultKey = new ResultKey(DATE_ONE, new Dictionary<string, string>(REGION_EU));
            repository.Save(resultKey, resultsWithMixedValues);

            AnalyzerContext loadedAnalyzerContext = repository.LoadByKey(resultKey).Value;


            loadedAnalyzerContext
                .SuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>())
                .ShouldBe(resultsWithSuccessfulValues.SuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>()));
        }
    }
}
