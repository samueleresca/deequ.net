using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Analyzers.States;
using xdeequ.AnomalyDetection;
using xdeequ.Checks;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Repository.InMemory;
using xdeequ.Util;
using Xunit;
using Xunit.Abstractions;

namespace xdeequ.tests
{
    [Collection("Spark instance")]
    public class VerificationSuiteTest
    {
        public VerificationSuiteTest(SparkFixture fixture) => _session = fixture.Spark;

        public SparkSession _session;

        private static void Evaluate(SparkSession session, Action<VerificationResult> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            IEnumerable<IAnalyzer<IMetric>> analyzers = GetAnalyzers();
            IEnumerable<Check> checks = GetChecks();

            VerificationResult results = new VerificationSuite()
                .OnData(data)
                .AddRequiredAnalyzer(analyzers)
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

        private static void AssertStatusFor(DataFrame df, Check[] checks, CheckStatus expectedStatus)
        {
            CheckStatus verificationSuiteStatus = new VerificationSuite()
                .OnData(df)
                .AddChecks(checks)
                .Run().Status;

            verificationSuiteStatus.ShouldBe(expectedStatus);
        }

        private static void AssertStatusFor(DataFrame df, Check check, CheckStatus expectedStatus)
        {
            CheckStatus verificationSuiteStatus = new VerificationSuite()
                .OnData(df)
                .AddCheck(check)
                .Run().Status;

            verificationSuiteStatus.ShouldBe(expectedStatus);
        }

        [Fact]
        public void accept_analysis_config_for_mandatory_analysis()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);

            CheckWithLastConstraintFilterable checkToSucceed = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None)
                .HasCompleteness("att1", _ => _ == 1.0, Option<string>.None);

            IAnalyzer<IMetric>[] analyzers =
            {
                new Size(Option<string>.None), new Completeness("att2"), new Uniqueness(new[] {"att2"}),
                new MutualInformation(new[] {"att1", "att2"})
            };

            VerificationResult result = new VerificationSuite().OnData(df).AddCheck(checkToSucceed)
                .AddRequiredAnalyzer(analyzers).Run();

            DataFrame analysisDf = new AnalyzerContext(result.Metrics)
                .SuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>());


            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"Dataset", "*", "Size", 4.0}),
                new GenericRow(new object[] {"Column", "att1", "Completeness", 1.0}),
                new GenericRow(new object[] {"Column", "att2", "Completeness", 1.0}),
                new GenericRow(new object[] {"Multicolumn", "att1,att2", "Uniqueness", 0.25}),
                new GenericRow(new object[]
                {
                    "Multicolumn", "att1,att2", "MutualInformation",
                    -(0.75 * Math.Log(0.75) + 0.25 * Math.Log(0.25))
                })
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("entity", new StringType()),
                    new StructField("instance", new StringType()),
                    new StructField("name", new StringType()),
                    new StructField("value", new DoubleType())
                });

            DataFrame expected = _session.CreateDataFrame(elements, schema);

            FixtureSupport.AssertSameRows(analysisDf, expected, Option<ITestOutputHelper>.None);
        }

        [Fact]
        public void only_append_results_to_repository_without_unnecessarily_overwriting_existing_ones()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();
            ResultKey resultKey = new ResultKey(0, new Dictionary<string, string>());
            IAnalyzer<IMetric>[] analyzers = { new Size(Option<string>.None), new Completeness("item") };

            Dictionary<IAnalyzer<IMetric>, IMetric> metrics = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run().Metrics;

            AnalyzerContext analyzerContext = new AnalyzerContext(metrics);

            new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(new Size(Option<string>.None))
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run();

            new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(new Completeness("item"))
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run();

            analyzerContext.Equals(repository.LoadByKey(resultKey).Value).ShouldBeTrue();
        }


        [Fact]
        public void return_the_correct_verification_status_regardless_of_the_order_of_checks()
        {
            DataFrame df = FixtureSupport.GetDfCompleteAndInCompleteColumns(_session);

            CheckWithLastConstraintFilterable checkToSucceed = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None)
                .HasCompleteness("att1", _ => _ == 1.0, Option<string>.None);

            CheckWithLastConstraintFilterable checkToErrorOut = new Check(CheckLevel.Error, "group-2-E")
                .HasCompleteness("att2", _ => _ > 0.8, Option<string>.None);

            CheckWithLastConstraintFilterable checkToWarn = new Check(CheckLevel.Warning, "group-2-W")
                .HasCompleteness("item", _ => _ < 0.8, Option<string>.None);


            AssertStatusFor(df, checkToSucceed, CheckStatus.Success);
            AssertStatusFor(df, checkToErrorOut, CheckStatus.Error);
            AssertStatusFor(df, checkToWarn, CheckStatus.Warning);


            AssertStatusFor(df, new[] { checkToSucceed, checkToErrorOut }, CheckStatus.Error);
            AssertStatusFor(df, new[] { checkToSucceed, checkToWarn }, CheckStatus.Warning);
            AssertStatusFor(df, new[] { checkToWarn, checkToErrorOut }, CheckStatus.Error);
            AssertStatusFor(df, new[] { checkToSucceed, checkToErrorOut, checkToWarn }, CheckStatus.Error);
        }


        [Fact]
        public void reuse_existing_results()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            Distinctness analyzerToTestReusingResults = new Distinctness(new[] { "att1", "att2" });


            VerificationResult verificationResult = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzerToTestReusingResults)
                .Run();

            AnalyzerContext analysisResult = new AnalyzerContext(verificationResult.Metrics);
            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();
            ResultKey resultKey = new ResultKey(0, new Dictionary<string, string>());

            repository.Save(resultKey, analysisResult);

            IAnalyzer<IMetric>[] analyzers =
            {
                analyzerToTestReusingResults, new Uniqueness(new[] {"item", "att2"}, Option<string>.None)
            };

            IEnumerable<IMetric> separateResults = analyzers.Select(x => x.Calculate(df));

            Dictionary<IAnalyzer<IMetric>, IMetric>.ValueCollection runnerResults = new VerificationSuite()
                .OnData(df)
                .UseRepository(repository)
                .ReuseExistingResultsForKey(resultKey)
                .AddRequiredAnalyzer(analyzers)
                .Run().Metrics.Values;


            separateResults
                .OrderBy(x => x.Name)
                .Select(x => (DoubleMetric)x)
                .SequenceEqual(runnerResults
                    .OrderBy(x => x.Name)
                    .Select(x => (DoubleMetric)x)
                ).ShouldBeTrue();
        }

        [Fact]
        public void run_the_analysis_even_there_are_no_constraints()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);

            VerificationResult result = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(new Size(Option<string>.None))
                .Run();

            result.Status.ShouldBe(CheckStatus.Success);

            DataFrame analysisDf = new AnalyzerContext(result.Metrics)
                .SuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>());

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"Dataset", "*", "Size", 4.0})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("entity", new StringType()),
                    new StructField("instance", new StringType()),
                    new StructField("name", new StringType()),
                    new StructField("value", new DoubleType())
                });

            DataFrame expected = _session.CreateDataFrame(elements, schema);
            FixtureSupport.AssertSameRows(analysisDf, expected, Option<ITestOutputHelper>.None);
        }

        [Fact]
        public void save_results_if_specified()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();
            ResultKey resultKey = new ResultKey(0, new Dictionary<string, string>());
            IAnalyzer<IMetric>[] analyzers = { new Size(Option<string>.None), new Completeness("item") };

            Dictionary<IAnalyzer<IMetric>, IMetric> metrics = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run().Metrics;

            AnalyzerContext analyzerContext = new AnalyzerContext(metrics);


            analyzerContext.Equals(repository.LoadByKey(resultKey).Value).ShouldBeTrue();
        }


        [Fact]
        public void if_there_are_previous_results_in_the_repository_new_results_should_pre_preferred_in_case_of_conflicts()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();
            ResultKey resultKey = new ResultKey(0, new Dictionary<string, string>());
            IAnalyzer<IMetric>[] analyzers = { new Size(Option<string>.None), new Completeness("item") };

            var actualResult = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run();

            var expectedAnalyzerContextOnLoadByKey = new AnalyzerContext(actualResult.Metrics);

            AnalyzerContext resultWhichShouldBeOverwritten = new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>
            {
                {new Size(Option<string>.None), new DoubleMetric(Entity.Dataset, "", "",  Try<double>.From(()=>100.0)) }
            });

            repository.Save(resultKey, resultWhichShouldBeOverwritten);

            new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey)
                .Run();


            var expected = expectedAnalyzerContextOnLoadByKey
                .MetricMap
                .ToDictionary(pair => pair.Key, pair => (DoubleMetric)pair.Value);

            var actual = repository.LoadByKey(resultKey)
                .Value
                .MetricMap
                .ToDictionary(pair => pair.Key, pair => (DoubleMetric)pair.Value);

            actual.OrderBy(x => x.Key.ToString())
                .SequenceEqual(expected.OrderBy(x => x.Key.ToString()));

        }

        [Fact]
        public void addAnomalyCheck_should_work()
        {
            DataFrame df = FixtureSupport.GetDFWithNRows(_session, 11);

            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();
            ResultKey resultKey = new ResultKey(5, new Dictionary<string, string>());
            IAnalyzer<IMetric>[] analyzers = { new Completeness("item") };

            var verificationResultOne = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey)
                .AddAnomalyCheck(new AbsoluteChangeStrategy(-2.0, 2.0),
                    new Size(Option<string>.None),
                    new AnomalyCheckConfig(CheckLevel.Warning, "Anomaly check to fail"))
                .Run();

            var verificationResultTwo = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey)
                .AddAnomalyCheck(new AbsoluteChangeStrategy(-7.0, 7.0),
                    new Size(Option<string>.None),
                    new AnomalyCheckConfig(CheckLevel.Error, "Anomaly check to succeed",
                        new Dictionary<string, string>(), 0, 11))
                .Run();

            var verificationResultThree = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey)
                .AddAnomalyCheck(
                    new AbsoluteChangeStrategy(-7.0, 7.0),
                    new Size(Option<string>.None), Option<AnomalyCheckConfig>.None
                )
                .Run();

            var checkResultsOne = verificationResultOne.CheckResults.First().Value;
            var checkResultsTwo = verificationResultTwo.CheckResults.First().Value;
            var checkResultsThree = verificationResultThree.CheckResults.First().Value;

            checkResultsOne.Status.ShouldBe(CheckStatus.Warning);
            checkResultsTwo.Status.ShouldBe(CheckStatus.Success);
            checkResultsThree.Status.ShouldBe(CheckStatus.Success);
        }
    }
}
