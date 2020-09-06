using System;
using System.Collections.Generic;
using System.Linq;
using deequ;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.AnomalyDetection;
using deequ.Checks;
using deequ.Extensions;
using deequ.Repository.InMemory;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using Xunit;
using Xunit.Abstractions;
using static deequ.Analyzers.Initializers;


namespace xdeequ.tests.Examples
{
    [Collection("Spark instance")]
    public class Examples
    {
        private readonly SparkSession _session;
        private readonly ITestOutputHelper _helper;

        public Examples(SparkFixture fixture, ITestOutputHelper helper)
        {
            _session = fixture.Spark;
            _helper = helper;
        }

        [Fact]
        public void should_execute_a_basic_example()
        {
            var data = _session.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {1, "Thingy A", "awesome thing. http://thingb.com", "high", 0}),
                    new GenericRow(new object[] {2, "Thingy B", "available at http://thingb.com", null, 0}),
                    new GenericRow(new object[] {3, null, null, "low", 5}),
                    new GenericRow(new object[] {4, "Thingy D", "checkout https://thingd.ca", "low", 10}),
                    new GenericRow(new object[] {5, "Thingy E", null, "high", 12})
                },
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()),
                    new StructField("productName", new StringType()),
                    new StructField("description", new StringType()),
                    new StructField("priority", new StringType()),
                    new StructField("numViews", new IntegerType()),
                }));

            var result = new VerificationSuite()
                .OnData(data)
                .AddCheck(
                    new Check(CheckLevel.Error, "integrity checks")
                        .HasSize(val => val == 5)
                        .IsComplete("id")
                        .IsUnique("id")
                        .IsComplete("productName")
                        .IsContainedIn("priority", new[] { "high", "low" })
                        .IsNonNegative("numViews")
                )
                .AddCheck(
                    new Check(CheckLevel.Warning, "distribution checks")
                        .ContainsURL("description", val => val >= .5)
                )
                .Run();

            result.Debug(_helper.WriteLine);
        }

        [Fact]
        public void should_execute_incremental_metrics_example()
        {
            DataFrame dataSetDE = LoadIncrementalMetricsData(
                new[] {new object[] {1, "ManufacturerA", "DE"}, new object[] {2, "ManufacturerB", "DE"},
                    new object[] {2, "ManufacturerC", "DE"}});

            DataFrame dataSetUS = LoadIncrementalMetricsData(
                new[]
                {
                    new object[] {3, "ManufacturerD", "US"}, new object[] {4, "ManufacturerE", "US"},
                    new object[] {5, "ManufacturerF", "US"}
                });

            DataFrame dataSetCN = LoadIncrementalMetricsData(
                new[] { new object[] { 6, "ManufacturerG", "CN" }, new object[] { 7, "ManufacturerH", "CN" }, });

            // We initialize a new check for the following data fields
            var check = new Check(CheckLevel.Warning, "generic check")
                .IsComplete("manufacturerName")
                .ContainsURL("manufacturerName", val => val == 0.0)
                .IsContainedIn("countryCode", new[] { "DE", "US", "CN" });


            // We create a new Analysis instance with the corresponding RequiredAnalyzers defined in the check
            Analysis analysis = new Analysis(check.RequiredAnalyzers());

            // We create a new in-memory state provider for each countryCode defined in the dataset
            InMemoryStateProvider deStates = new InMemoryStateProvider();
            InMemoryStateProvider usStates = new InMemoryStateProvider();
            InMemoryStateProvider cnStates = new InMemoryStateProvider();

            // These call will store the resulting metrics in the separate states providers for each dataSet
            AnalysisRunner.Run(dataSetDE, analysis, saveStatesWith: deStates);
            AnalysisRunner.Run(dataSetUS, analysis, saveStatesWith: usStates);
            AnalysisRunner.Run(dataSetCN, analysis, saveStatesWith: cnStates);

            // Next, we are able to compute the metrics for the whole table from the partition states
            // This just aggregates the previously calculated metrics, it doesn't performs computation on the data
            AnalyzerContext tableMetrics = AnalysisRunner.RunOnAggregatedStates(dataSetDE.Schema(), analysis,
                new[] { deStates, usStates, cnStates });

            // Lets now assume that a single partition changes. We only need to recompute the state of this
            // partition in order to update the metrics for the whole table.
            DataFrame updatedUsManufacturers = LoadIncrementalMetricsData(new[]
            {
                new object[] {3, "ManufacturerDNew", "US"}, new object[] {4, null, "US"},
                new object[] {5, "ManufacturerFNew http://clickme.com", "US"},
            });

            // Recompute state of partition
            InMemoryStateProvider updatedUsStates = new InMemoryStateProvider();
            AnalysisRunner.Run(updatedUsManufacturers, analysis, updatedUsStates);

            // Recompute metrics for whole tables from states. We do not need to touch old data!
            AnalyzerContext updatedTableMetrics = AnalysisRunner.RunOnAggregatedStates(dataSetDE.Schema(), analysis,
                new[] { deStates, usStates, cnStates });
        }


        [Fact]
        public void should_execute_anomaly_detection_example()
        {
            // Anomaly detection operates on metrics stored in a metric repository, so lets create one
            InMemoryMetricsRepository metricsRepository = new InMemoryMetricsRepository();
            // This is the key which we use to store the metrics for the dataset from yesterday
            ResultKey yesterdayKeys =
                new ResultKey(DateTime.Now.Ticks - 24 * 60 * 1000);
            /* In this simple example, we assume that we compute metrics on a dataset every day and we want
           to ensure that they don't change drastically. For sake of simplicity, we just look at the
           size of the data */

            /* Yesterday, the data had only two rows */
            var yesterdaysDataset = LoadAnomalyDetectionData(new List<object[]>
            {
                new object[] {1, "Thingy A", "awesome thing.", "high", 0},
                new object[] {2, "Thingy B", "available at http://thingb.com", null, 0}
            });
            /* We test for anomalies in the size of the data, it should not increase by more than 2x. Note
               that we store the resulting metrics in our repository */
            new VerificationSuite()
                .OnData(yesterdaysDataset)
                .UseRepository(metricsRepository)
                .SaveOrAppendResult(yesterdayKeys)
                .AddAnomalyCheck(
                    new RelativeRateOfChangeStrategy(maxRateIncrease: 2.0),
                    Size()
                )
                .Run()
                .Debug(_helper.WriteLine);


            /* Todays data has five rows, so the data size more than doubled and our anomaly check should
              catch this */
            var todaysDataset = LoadAnomalyDetectionData(new List<object[]>
            {
                new object[] {1, "Thingy A", "awesome thing.", "high", 0},
                new object[] {2, "Thingy B", "available at http://thingb.com", null, 0},
                new object[] {3, null, null, "low", 5},
                new object[] {4, "Thingy D", "checkout https://thingd.ca", "low", 10},
                new object[] {5, "Thingy W", null, "high", 12}
            });


            /* The key for today's result */
            var todaysKey = new ResultKey(DateTime.Now.Ticks - 24 * 60 * 1000);

            /* Repeat the anomaly check for today's data */
            var verificationResult = new VerificationSuite()
                .OnData(todaysDataset)
                .UseRepository(metricsRepository)
                .SaveOrAppendResult(todaysKey)
                .AddAnomalyCheck(
                    new RelativeRateOfChangeStrategy(maxRateIncrease:2.0),
                    Size()
                )
                .Run();

            verificationResult.Status.ShouldBe(CheckStatus.Warning);

            _helper.WriteLine("Anomaly detected in the Size() metric!");

            /* Lets have a look at the actual metrics. */
            metricsRepository
                .Load()
                .ForAnalyzers(new[] { Size() })
                .GetSuccessMetricsAsDataFrame(_session)
                .Show();
        }

        private DataFrame LoadIncrementalMetricsData(IEnumerable<object[]> rows) =>
            _session.CreateDataFrame(
                rows.Select(row => new GenericRow(row)),
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()),
                    new StructField("manufacturerName", new StringType()),
                    new StructField("countryCode", new StringType())
                }));


        private static DataFrame LoadAnomalyDetectionData(IEnumerable<object[]> rows) =>
            SparkSession.Builder().GetOrCreate().CreateDataFrame(
                rows.Select(row => new GenericRow(row)),
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()),
                    new StructField("productName", new StringType()),
                    new StructField("description", new StringType()),
                    new StructField("priority", new StringType()),
                    new StructField("numViews", new IntegerType()),
                }));
    }
}
