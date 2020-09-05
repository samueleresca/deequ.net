# Anomaly detection

The anomaly detection feature help us to monitor the metric variation over time.
Anomaly checks compare the current value of the metric to its values in the past and allow us to detect anomalous changes.
The feature relies on the process of storing the metric result using the `MetricRepository`.
In this simple example, we assume that we compute the size of a dataset every day and we want to ensure that it does not change drastically: the number of rows on a given day should not be more than double of what we have seen on the day before.

First of all, we need to create a new `IMetricRepository` that stores the previous metrics:

```c#
IMetricsRepository metricsRepository = new InMemoryMetricsRepository();
```

For the purpose of the example, we need to use fictitious data that we can use to compute the metrics for the past day:
```c#
    DataSet yesterdaysDataset = LoadData(new List<object[]>
    {
        new object[] {1, "Thingy A", "awesome thing.", "high", 0},
        new object[] {2, "Thingy B", "available at http://thingb.com", null, 0}
    });

    var yesterdayKeys =
                new ResultKey(new DateTime().Ticks - 24 * 60 * 1000);

```
The following `VerificationSuite` uses the API provided by deequ for testing the anomalies in the size of the data.
The data should not increase by more than 2x. The below check uses the `RelativeRateOfChangeStrategy` class for configuring and detecting the anomaly.
Note that the `VerificationSuite` store the resulting metrics in our repository via the `UseRepository` and the `SaveOrAppendResult` methods using the `yestardayKey`.
```c#

    new VerificationSuite()
        .OnData(yesterdaysDataset)
        .UseRepository(metricsRepository)
        .SaveOrAppendResult(yesterdayKeys)
        .AddAnomalyCheck(
            new RelativeRateOfChangeStrategy(2.0),
            Size()
        )
        .Run();
```

Once the metrics for the previous day are correctly stored by in memory, we can proceed by creating a new fictitious dataset for today:

```c#

 DataFrame todaysDataset = LoadData(new List<object[]>
    {
        new object[] {1, "Thingy A", "awesome thing.", "high", 0},
        new object[] {2, "Thingy B", "available at http://thingb.com", null, 0},
        new object[] {3, null, null, "low", 5},
        new object[] {4, "Thingy D", "checkout https://thingd.ca", "low", 10},
        new object[] {5, "Thingy W", null, "high", 12}
    });

   var todaysKey = new ResultKey(new DateTime().Ticks - 24 * 60 * 1000);
```
Now it is necessary repeat the anomaly check using the same metrics repository:

```c#
    var verificationResult = new VerificationSuite()
        .OnData(todaysDataset)
        .UseRepository(metricsRepository)
        .SaveOrAppendResult(todaysKey)
        .AddAnomalyCheck(
            new RelativeRateOfChangeStrategy(maxRateIncrease:2.0),
            Size()
        )
        .Run()
        .Debug();
```

Once the `VerificationSuite` completes, the `VerificationResult` status should contain an warning because the dataset increased with a ratio greater than 2x:

```c#
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
```

An [executable version of this example](AnomalyDetection.cs) is part of the codebase.
