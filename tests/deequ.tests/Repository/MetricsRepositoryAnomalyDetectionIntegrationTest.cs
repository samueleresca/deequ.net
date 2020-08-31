using System;
using System.Collections.Generic;
using System.Linq;
using deequ;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.AnomalyDetection;
using deequ.Checks;
using deequ.Constraints;
using deequ.Metrics;
using deequ.Repository;
using deequ.Repository.InMemory;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class MetricsRepositoryAnomalyDetectionIntegrationTest
    {
        private readonly SparkSession _session;
        private readonly ITestOutputHelper _helper;

        public MetricsRepositoryAnomalyDetectionIntegrationTest(SparkFixture fixture, ITestOutputHelper helper)
        {
            _session = fixture.Spark;
            _helper = helper;
        }

        [Fact(Skip = "TODO")]
        public void work_using_the_InMemoryMetricsRepository()
        {
            var repository = new InMemoryMetricsRepository();

            TestAnomalyDetection(_session, repository);
        }

        private void TestAnomalyDetection(
            SparkSession session,
            IMetricsRepository repository)
        {
            var data = GetTestData(session);

            // Fill repository with some fake results from previous runs for July 2018
            FillRepositoryWithPreviousResults(repository);

            // Some other checks and analyzers we are interested in not related to the anomaly detection
            var (otherCheck, additionalRequiredAnalyzers) = GetNormalCheckAndRequiredAnalyzers();

            // This method is where the interesting stuff happens
            var verificationResult = CreateAnomalyChecksAndRunEverything(data, repository, otherCheck,
                additionalRequiredAnalyzers);

            PrintConstraintResults(verificationResult);

            AssertAnomalyCheckResultsAreCorrect(verificationResult);
        }

        private DataFrame GetTestData(SparkSession session)
        {
            var schema = new StructType(new[]
            {
                new StructField("item", new StringType()), new StructField("origin", new StringType()),
                new StructField("sales", new IntegerType()), new StructField("marketplace", new StringType())
            });


            var rowData = new List<GenericRow>
            {
                new GenericRow(new object[] {"item1", "US", 100, "EU"}),
                new GenericRow(new object[] {"item1", "US", 1000, "EU"}),
                new GenericRow(new object[] {"item1", "US", 20, "EU"}),
                new GenericRow(new object[] {"item2", "DE", 20, "EU"}),
                new GenericRow(new object[] {"item2", "DE", 333, "EU"}),
                new GenericRow(new object[] {"item3", null, 12, "EU"}),
                new GenericRow(new object[] {"item4", null, 45, "EU"}),
                new GenericRow(new object[] {"item5", null, 123, "EU"})
            };

            return session.CreateDataFrame(rowData, schema);
        }

        private void FillRepositoryWithPreviousResults(IMetricsRepository repository)
        {
            Enumerable.Range(1, 31).Select(pastDay =>
            {
                var pastResultsEU = new Dictionary<IAnalyzer<IMetric>, IMetric>
                {
                    {Initializers.Size(), new DoubleMetric(Entity.Dataset, "*", "Size", Math.Floor(pastDay / 3.0))},
                    {Initializers.Mean("sales"), new DoubleMetric(Entity.Column, "sales", "Mean", pastDay * 7)}
                };

                var pastResultsNA = new Dictionary<IAnalyzer<IMetric>, IMetric>
                {
                    {Initializers.Size(), new DoubleMetric(Entity.Dataset, "*", "Size", pastDay)},
                    {Initializers.Mean("sales"), new DoubleMetric(Entity.Column, "sales", "Mean", pastDay * 9)}
                };

                var analyzerContextEU = new AnalyzerContext(pastResultsEU);
                var analyzerContextNA = new AnalyzerContext(pastResultsNA);

                long dateTime = CreateDate(2018, 7, pastDay);

                repository.Save(new ResultKey(dateTime, new Dictionary<string, string> { { "marketplace", "EU" } }),
                    analyzerContextEU);

                repository.Save(new ResultKey(dateTime, new Dictionary<string, string> { { "marketplace", "NA" } }),
                    analyzerContextNA);

                return pastDay;
            });
        }

        private (Check, IEnumerable<IAnalyzer<IMetric>>) GetNormalCheckAndRequiredAnalyzers()
        {
            var check = new Check(CheckLevel.Error, "check")
                .IsComplete("item")
                .IsComplete("origin")
                .IsContainedIn("marketplace", new[] { "EU" })
                .IsNonNegative("sales");

            var requiredAnalyzers =
                new IAnalyzer<IMetric>[] { Initializers.Maximum("sales"), Initializers.Minimum("sales") };

            return (check, requiredAnalyzers);
        }

        private VerificationResult CreateAnomalyChecksAndRunEverything(
            DataFrame data,
            IMetricsRepository repository,
            Check otherCheck,
            IEnumerable<IAnalyzer<IMetric>> additionalRequiredAnalyzers)
        {
            // We only want to use historic data with the EU tag for the anomaly checks since the new
            // data point is from the EU marketplace
            var filterEU = new Dictionary<string, string> { { "marketplace", "EU" } };

            // We only want to use data points before the date time associated with the current
            // data point and only ones that are from 2018
            var afterDateTime = CreateDate(2018, 1, 1);
            var beforeDateTime = CreateDate(2018, 8, 1);

            // Config for the size anomaly check
            var sizeAnomalyCheckConfig = new AnomalyCheckConfig(CheckLevel.Error, "Size only increases",
                filterEU, afterDateTime, beforeDateTime);
            var sizeAnomalyDetectionStrategy = new AbsoluteChangeStrategy(0);

            // Config for the mean sales anomaly check
            var meanSalesAnomalyCheckConfig = new AnomalyCheckConfig(
                CheckLevel.Warning,
                "Sales mean within 2 standard deviations",
                filterEU,
                afterDateTime,
                beforeDateTime
            );

            var meanSalesAnomalyDetectionStrategy = new OnlineNormalStrategy(upperDeviationFactor: 2, lowerDeviationFactor: Option<double>.None,
                ignoreAnomalies: false);

            // ResultKey to be used when saving the results of this run
            var currentRunResultKey =
                new ResultKey(CreateDate(2018, 8, 1), new Dictionary<string, string> { { "marketplace", "EU" } });


            return new VerificationSuite()
                .OnData(data)
                .AddCheck(otherCheck)
                .AddRequiredAnalyzers(additionalRequiredAnalyzers)
                .UseRepository(repository)
                // Add the Size anomaly check
                .AddAnomalyCheck(sizeAnomalyDetectionStrategy, Initializers.Size(), sizeAnomalyCheckConfig)
                // Add the Mean sales anomaly check
                .AddAnomalyCheck(meanSalesAnomalyDetectionStrategy, Initializers.Mean("sales"),
                    meanSalesAnomalyCheckConfig)
                // Save new data point in the repository after we calculated everything
                .SaveOrAppendResult(currentRunResultKey)
                .Run();
        }

        private void AssertAnomalyCheckResultsAreCorrect(VerificationResult verificationResult)
        {
            // New size value is 8, that is an anomaly because it is lower than the last value, 10
            var sizeAnomalyCheckWithResult = verificationResult.CheckResults
                .First(keyValue => keyValue.Key.Description == "Size only increases");

            var (_, checkResultSizeAnomalyCheck) = sizeAnomalyCheckWithResult;


            CheckStatus.Error.ShouldBe(checkResultSizeAnomalyCheck.Status);

            // New Mean sales value is 206.625, that is not an anomaly because the previous values are
            // (1 to 30) * 7 and it is within the range of 2 standard deviations
            // (mean: ~111, stdDeviation: ~62)
            var meanSalesAnomalyCheckWithResult = verificationResult.CheckResults
                .First(keyValue => keyValue.Key.Description == "Sales mean within 2 standard deviations");


            var (_, checkResultMeanSalesAnomalyCheck) = meanSalesAnomalyCheckWithResult;

            CheckStatus.Success.ShouldBe(checkResultMeanSalesAnomalyCheck.Status);
        }

        private void PrintConstraintResults(VerificationResult result)
        {
            _helper.WriteLine("\n\n### CONSTRAINT RESULTS ###");
            _helper.WriteLine("\n\t--- Successful constraints ---");

            result.CheckResults.ToList().ForEach(keyValue =>
                keyValue.Value.ConstraintResults
                    .Where(constraintResult => constraintResult.Status == ConstraintStatus.Success)
                    .ToList().ForEach(value => _helper.WriteLine($"{value.Constraint}")));

            _helper.WriteLine("\n\t--- Failed constraints ---");
            result.CheckResults.ToList().ForEach(keyValue =>
                keyValue.Value.ConstraintResults
                    .Where(constraintResult => constraintResult.Status != ConstraintStatus.Success)
                    .ToList().ForEach(value => _helper.WriteLine($"{value.Constraint}:{value.Message.GetOrElse("")}")));
        }

        private long CreateDate(int year, int month, int day)
        {
            return new DateTime(year, month, day, 0, 0, 0).Ticks;
        }
    }
}
