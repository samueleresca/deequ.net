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
                .HasDistinctness(new[] {"item"}, _ => _ < 0.8, "Should be smaller than 0.8!");


            return new[] {checkToSucceed, checkToErrorOut, checkToWarn};
        }

        private static IEnumerable<IAnalyzer<IMetric>> GetAnalyzers() =>
            new[] {Initializers.Uniqueness(new[] {"att1", "att2"})};

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
            var verificationSuiteStatus = new VerificationSuite()
                .OnData(df)
                .AddChecks(checks)
                .Run().Status;

            verificationSuiteStatus.ShouldBe(expectedStatus);
        }

        private static void AssertStatusFor(DataFrame df, Check check, CheckStatus expectedStatus)
        {
            var verificationSuiteStatus = new VerificationSuite()
                .OnData(df)
                .AddCheck(check)
                .Run().Status;

            verificationSuiteStatus.ShouldBe(expectedStatus);
        }


        [Fact]
        public void return_the_correct_verification_status_regardless_of_the_order_of_checks()
        {
            DataFrame df = FixtureSupport.GetDfCompleteAndInCompleteColumns(_session);

            var checkToSucceed = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None)
                .HasCompleteness("att1", _ => _ == 1.0, Option<string>.None);

            var checkToErrorOut = new Check(CheckLevel.Error, "group-2-E")
                .HasCompleteness("att2", _ => _ > 0.8, Option<string>.None);

            var checkToWarn = new Check(CheckLevel.Warning, "group-2-W")
                .HasCompleteness("item", _ => _ < 0.8, Option<string>.None);


            AssertStatusFor(df, checkToSucceed, CheckStatus.Success);
            AssertStatusFor(df, checkToErrorOut, CheckStatus.Error);
            AssertStatusFor(df, checkToWarn, CheckStatus.Warning);


            AssertStatusFor(df, new[] {checkToSucceed, checkToErrorOut}, CheckStatus.Error);
            AssertStatusFor(df, new[] {checkToSucceed, checkToWarn}, CheckStatus.Warning);
            AssertStatusFor(df, new[] {checkToWarn, checkToErrorOut}, CheckStatus.Error);
            AssertStatusFor(df, new[] {checkToSucceed, checkToErrorOut, checkToWarn}, CheckStatus.Error);
        }

        [Fact]
        public void accept_analysis_config_for_mandatory_analysis()
        {
            var df = FixtureSupport.GetDFFull(_session);

            var checkToSucceed = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None)
                .HasCompleteness("att1", _ => _ == 1.0, Option<string>.None);

            var analyzers = new IAnalyzer<IMetric>[]
            {
                new Size(Option<string>.None), new Completeness("att2"), new Uniqueness(new[] {"att2"}),
                new MutualInformation(new[] {"att1", "att2"})
            };

            var result = new VerificationSuite().OnData(df).AddCheck(checkToSucceed)
                .AddRequiredAnalyzer(analyzers).Run();

            var analysisDf = new AnalyzerContext(result.Metrics)
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
        public void run_the_analysis_even_there_are_no_constraints()
        {
            var df = FixtureSupport.GetDFFull(_session);

            var result = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(new Size(Option<string>.None))
                .Run();

            result.Status.ShouldBe(CheckStatus.Success);

            var analysisDf = new AnalyzerContext(result.Metrics)
                .SuccessMetricsAsDataFrame(_session, Enumerable.Empty<IAnalyzer<IMetric>>());

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"Dataset", "*", "Size", 4.0}),
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
        public void reuse_existing_results()
        {
            var df = FixtureSupport.GetDfWithNumericValues(_session);

            var analyzerToTestReusingResults = new Distinctness(new[] {"att1", "att2"});


            var verificationResult = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzerToTestReusingResults)
                .Run();

            var analysisResult = new AnalyzerContext(verificationResult.Metrics);
            var repository = new InMemoryMetricsRepository();
            var resultKey = new ResultKey(0, new Dictionary<string, string>());

            repository.Save(resultKey, analysisResult);

            var analyzers = new IAnalyzer<IMetric>[]
            {
                analyzerToTestReusingResults, new Uniqueness(new[] {"item", "att2"}, Option<string>.None)
            };

            var separateResults = analyzers.Select(x => x.Calculate(df));

            var runnerResults = new VerificationSuite()
                .OnData(df)
                .UseRepository(repository)
                .ReuseExistingResultsForKey(resultKey)
                .AddRequiredAnalyzer(analyzers)
                .Run().Metrics.Values;


            separateResults
                .OrderBy(x=>x.Name)
                .Select(x=> (DoubleMetric) x)
                .SequenceEqual(runnerResults
                    .OrderBy(x=>x.Name)
                    .Select(x=> (DoubleMetric) x)
                ).ShouldBeTrue();
        }

        [Fact]
        public void save_results_if_specified()
        {

            var df = FixtureSupport.GetDfWithNumericValues(_session);

                var repository = new InMemoryMetricsRepository();
                var resultKey = new ResultKey(0, new Dictionary<string, string>());
                var analyzers = new IAnalyzer<IMetric>[] {new Size(Option<string>.None), new Completeness("item")};

                var metrics = new VerificationSuite()
                    .OnData(df)
                    .AddRequiredAnalyzer(analyzers)
                    .UseRepository(repository)
                    .SaveOrAppendResult(resultKey).Run().Metrics;

                var analyzerContext = new AnalyzerContext(metrics);


                analyzerContext.Equals(repository.LoadByKey(resultKey).Value).ShouldBeTrue();
        }

        [Fact]
        public void only_append_results_to_repository_without_unnecessarily_overwriting_existing_ones()
        {

            var df = FixtureSupport.GetDfWithNumericValues(_session);

            var repository = new InMemoryMetricsRepository();
            var resultKey = new ResultKey(0, new Dictionary<string, string>());
            var analyzers = new IAnalyzer<IMetric>[] {new Size(Option<string>.None), new Completeness("item")};

            var metrics = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzer(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run().Metrics;

            var analyzerContext = new AnalyzerContext(metrics);

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

}
}
