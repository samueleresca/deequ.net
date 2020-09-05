using System;
using System.Collections.Generic;
using System.Linq;
using deequ;
using deequ.Analyzers.Runners;
using deequ.AnomalyDetection;
using deequ.Checks;
using deequ.Extensions;
using deequ.Repository;
using deequ.Repository.InMemory;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

using static deequ.Analyzers.Initializers;

namespace examples
{
    public static class AnomalyDetection
    {

        public static void AnomalyDetectionExample()
        {
            // Anomaly detection operates on metrics stored in a metric repository, so lets create one
            IMetricsRepository metricsRepository = new InMemoryMetricsRepository();
            // This is the key which we use to store the metrics for the dataset from yesterday
            ResultKey yesterdayKeys =
                new ResultKey(new DateTime().Ticks - 24 * 60 * 1000);
            /* In this simple example, we assume that we compute metrics on a dataset every day and we want
           to ensure that they don't change drastically. For sake of simplicity, we just look at the
           size of the data */

            /* Yesterday, the data had only two rows */
            var yesterdaysDataset = LoadData(new List<object[]>
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
                    new RelativeRateOfChangeStrategy(2.0),
                    Size()
                )
                .Run();


            /* Todays data has five rows, so the data size more than doubled and our anomaly check should
              catch this */
            var todaysDataset = LoadData(new List<object[]>
            {
                new object[] {1, "Thingy A", "awesome thing.", "high", 0},
                new object[] {2, "Thingy B", "available at http://thingb.com", null, 0},
                new object[] {3, null, null, "low", 5},
                new object[] {4, "Thingy D", "checkout https://thingd.ca", "low", 10},
                new object[] {5, "Thingy W", null, "high", 12}
            });


            /* The key for today's result */
            var todaysKey = new ResultKey(new DateTime().Ticks - 24 * 60 * 1000);

            /* Repeat the anomaly check for today's data */
            var verificationResult = new VerificationSuite()
                .OnData(todaysDataset)
                .UseRepository(metricsRepository)
                .SaveOrAppendResult(todaysKey)
                .AddAnomalyCheck(
                    new RelativeRateOfChangeStrategy(2.0),
                    Size()
                )
                .Run()
                .Debug();




            /* Did we find an anomaly? */
            if (verificationResult.Status != CheckStatus.Success)
            {
                Console.WriteLine("Anomaly detected in the Size() metric!");

                /* Lets have a look at the actual metrics. */
                metricsRepository
                    .Load()
                    .ForAnalyzers(new[] { Size() })
                    .GetSuccessMetricsAsDataFrame(SparkSession.Active())
                    .Show();
            }


        }

        private static DataFrame LoadData(IEnumerable<object[]> rows) =>
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


