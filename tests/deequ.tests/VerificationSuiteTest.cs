using System;
using System.Collections.Generic;
using System.Linq;
using deequ;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Analyzers.States;
using deequ.AnomalyDetection;
using deequ.Checks;
using deequ.Constraints;
using deequ.Metrics;
using deequ.Repository;
using deequ.Repository.InMemory;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Moq;
using Shouldly;
using Xunit;
using Xunit.Abstractions;
using static deequ.Constraints.Functions;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.tests
{
    [Collection("Spark instance")]
    public class VerificationSuiteTest
    {
        public VerificationSuiteTest(SparkFixture fixture) => _session = fixture.Spark;

        public SparkSession _session;

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


        private void EvaluateWithRepositoryWithHistory(Action<IMetricsRepository> callback)
        {
            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();

            for (int i = 1; i <= 2; i++)
            {
                AnalyzerContext analyzerContext = new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>
                {
                    {
                        new Size(Option<string>.None),
                        new DoubleMetric(Entity.Column, "", "", Try<double>.From(() => i))
                    }
                });

                repository.Save(new ResultKey(i, new Dictionary<string, string> { { "Region", "EU" } }), analyzerContext);
            }


            for (int i = 3; i <= 4; i++)
            {
                AnalyzerContext analyzerContext = new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>
                {
                    {
                        new Size(Option<string>.None),
                        new DoubleMetric(Entity.Column, "", "", Try<double>.From(() => i))
                    }
                });

                repository.Save(new ResultKey(i, new Dictionary<string, string> { { "Region", "NA" } }), analyzerContext);
            }

            callback(repository);
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
                .AddRequiredAnalyzers(analyzers).Run();

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
        public void addAnomalyCheck_should_work() =>
            EvaluateWithRepositoryWithHistory(repository =>
            {
                DataFrame df = FixtureSupport.GetDFWithNRows(_session, 11);

                ResultKey resultKey = new ResultKey(5, new Dictionary<string, string>());
                IAnalyzer<IMetric>[] analyzers = { new Completeness("item") };

                VerificationResult verificationResultOne = new VerificationSuite()
                    .OnData(df)
                    .UseRepository(repository)
                    .AddRequiredAnalyzers(analyzers)
                    .SaveOrAppendResult(resultKey)
                    .AddAnomalyCheck(new AbsoluteChangeStrategy(-2.0, 2.0),
                        new Size(Option<string>.None),
                        new AnomalyCheckConfig(CheckLevel.Warning, "Anomaly check to fail"))
                    .Run();

                VerificationResult verificationResultTwo = new VerificationSuite()
                    .OnData(df)
                    .AddRequiredAnalyzers(analyzers)
                    .UseRepository(repository)
                    .SaveOrAppendResult(resultKey)
                    .AddAnomalyCheck(new AbsoluteChangeStrategy(-7.0, 7.0),
                        new Size(Option<string>.None),
                        new AnomalyCheckConfig(CheckLevel.Error, "Anomaly check to succeed",
                            new Dictionary<string, string>(), 0, 11))
                    .Run();

                VerificationResult verificationResultThree = new VerificationSuite()
                    .OnData(df)
                    .AddRequiredAnalyzers(analyzers)
                    .UseRepository(repository)
                    .SaveOrAppendResult(resultKey)
                    .AddAnomalyCheck(
                        new AbsoluteChangeStrategy(-7.0, 7.0),
                        new Size(Option<string>.None), Option<AnomalyCheckConfig>.None
                    ).Run();

                CheckStatus checkResultsOne = verificationResultOne.CheckResults.First().Value.Status;
                CheckStatus checkResultsTwo = verificationResultTwo.CheckResults.First().Value.Status;
                CheckStatus checkResultsThree = verificationResultThree.CheckResults.First().Value.Status;


                checkResultsOne.ShouldBe(CheckStatus.Warning);
                checkResultsTwo.ShouldBe(CheckStatus.Success);
                checkResultsThree.ShouldBe(CheckStatus.Success);
            });


        [Fact]
        public void
            if_there_are_previous_results_in_the_repository_new_results_should_pre_preferred_in_case_of_conflicts()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();
            ResultKey resultKey = new ResultKey(0, new Dictionary<string, string>());
            IAnalyzer<IMetric>[] analyzers = { new Size(Option<string>.None), new Completeness("item") };

            VerificationResult actualResult = new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzers(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run();

            AnalyzerContext expectedAnalyzerContextOnLoadByKey = new AnalyzerContext(actualResult.Metrics);

            AnalyzerContext resultWhichShouldBeOverwritten = new AnalyzerContext(
                new Dictionary<IAnalyzer<IMetric>, IMetric>
                {
                    {
                        new Size(Option<string>.None),
                        new DoubleMetric(Entity.Dataset, "", "", Try<double>.From(() => 100.0))
                    }
                });

            repository.Save(resultKey, resultWhichShouldBeOverwritten);

            new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzers(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey)
                .Run();


            Dictionary<IAnalyzer<IMetric>, DoubleMetric> expected = expectedAnalyzerContextOnLoadByKey
                .MetricMap
                .ToDictionary(pair => pair.Key, pair => (DoubleMetric)pair.Value);

            Dictionary<IAnalyzer<IMetric>, DoubleMetric> actual = repository.LoadByKey(resultKey)
                .Value
                .MetricMap
                .ToDictionary(pair => pair.Key, pair => (DoubleMetric)pair.Value);

            actual.OrderBy(x => x.Key.ToString())
                .SequenceEqual(expected.OrderBy(x => x.Key.ToString()));
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
                .AddRequiredAnalyzers(analyzers)
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
                .AddRequiredAnalyzers(analyzers)
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
                .AddRequiredAnalyzers(analyzers)
                .UseRepository(repository)
                .SaveOrAppendResult(resultKey).Run().Metrics;

            AnalyzerContext analyzerContext = new AnalyzerContext(metrics);


            analyzerContext.Equals(repository.LoadByKey(resultKey).Value).ShouldBeTrue();
        }

        [Fact]
        public void call_state_persister_if_specified()
        {
            var statePersister = new Mock<IStatePersister>();

            statePersister
                .Setup(x => x
                    .Persist(It.IsAny<Option<IAnalyzer<IMetric>>>(), It.IsAny<IState>()));

            var df = FixtureSupport.GetDfWithNumericValues(_session);

            IAnalyzer<IMetric>[] analyzers = { new Sum("att2", Option<string>.None),
                new Completeness("att1")};

            IState[] states = { new SumState(18.0), new NumMatchesAndCount(6, 6) };

            new VerificationSuite()
                .OnData(df)
                .AddRequiredAnalyzers(analyzers)
                .SaveStateWith(statePersister.Object)
                .Run();

            var analyzerStateTuples = analyzers
                .Zip(states, (analyzer, state) => (analyzer, state));


            statePersister.Verify(x => x.Persist(
                    It.Is<Option<IAnalyzer<IMetric>>>(x => x.Value.ToString() == analyzers.First().ToString()),
                    It.Is<Option<SumState>>(x => x.Value.MetricValue() == 18)), Times.AtLeastOnce);

            statePersister.Verify(x => x.Persist(
                It.Is<Option<IAnalyzer<IMetric>>>(x => x.Value.ToString() == analyzers.Skip(1).First().ToString()),
                It.Is<Option<NumMatchesAndCount>>(x => x.Value.MetricValue() == 6 / 6)), Times.AtLeastOnce);
        }

        [Fact]
        public void load_stored_states_for_aggregation_if_specified()
        {
            var statePersister = new Mock<IStateLoader>();

            IAnalyzer<IMetric>[] analyzers = { new Sum("att2", Option<string>.None),
                new Completeness("att1")};
            statePersister
                .Setup(x => x
                    .Load<SumState>(
                        It.Is<Option<IAnalyzer<IMetric>>>(x => x.Value.ToString() == "Sum(att2,None)")
                        )).Returns(new SumState(18.0));

            statePersister
                .Setup(x => x
                    .Load<NumMatchesAndCount>(
                        It.Is<Option<IAnalyzer<IMetric>>>(x => x.Value.ToString() == "Completeness(att1,None)")
                    )).Returns(new NumMatchesAndCount(0, 6));


            var df = FixtureSupport.GetDfWithNumericValues(_session);

            var result = new VerificationSuite()
                 .OnData(df)
                 .AddRequiredAnalyzers(analyzers)
                 .AggregateWith(statePersister.Object)
                 .Run();

            var sumState = (DoubleMetric)result.Metrics[analyzers.First()];
            sumState.Value.Get().ShouldBe(18 * 2);
            var matches = (DoubleMetric)result.Metrics[analyzers.Skip(1).First()];
            matches.Value.Get().ShouldBe(0.5);
        }

        [Fact]
        public void keep_order_of_check_constraints_and_their_results()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);


            var expectedConstraints = new IConstraint[]
            {
                CompletenessConstraint("att1", _ => _ == 1.0, Option<string>.None, Option<string>.None),
                ComplianceConstraint("att1 is positive", Column("att1"), _ => _ == 1.0, Option<string>.None, Option<string>.None)
            };

            var check = expectedConstraints.Aggregate(new Check(CheckLevel.Error, "check"),
                (check, constraint) => check.AddConstraint(constraint));

            check.Constraints.SequenceEqual(expectedConstraints).ShouldBeTrue();
            check.Constraints.SequenceEqual(expectedConstraints.Reverse()).ShouldBeFalse();


            var results = new VerificationSuite()
                .OnData(df)
                .AddCheck(check)
                .Run();

            var checkConstraintsWithResultConstraints = check
                .Constraints.
                Zip(results.CheckResults[check].ConstraintResults, (check, result) => (check, result));

            foreach (var constraint in checkConstraintsWithResultConstraints)
            {
                constraint.check.ShouldBe(constraint.result.Constraint);
            }
        }
    }
}
