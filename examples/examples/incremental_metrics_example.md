# Incremental metrics computation

In a real-world scenario, ETLs usually import batches of data, and the data-sets continuously grow in size with new data.
Therefore, deeq.NET supports situations where the resulting metrics of the analyzers can be stored and calculated using an incremental approach.

The incremental computation method we described is achievable using the APIs exposed by deequ. The following example demonstrate how to implement the incremental computation using the following data sample:

| id | manufacturerName | countryCode |
|:--:|:----------------:|:-----------:|
|  1 |   ManufacturerA  |      DE     |
|  2 |   ManufacturerB  |      DE     |
|  3 |   ManufacturerC  |      DE     |
|  4 |   ManufacturerD  |      US     |
|  5 |   ManufacturerE  |      US     |
|  6 |   ManufacturerG  |      CN     |


As you can see, the data comes with a partition key, which is the `countryCode`. First, we build the dataset and implement the list of constraints want to apply to our data and we can continue by creating a new `Analysis` instance:

```c#
    DataFrame dataSetDE = LoadData(
        new[] {new object[] {1, "ManufacturerA", "DE"}, new object[] {2, "ManufacturerB", "DE"},
            new object[] {2, "ManufacturerC", "DE"}});

    DataFrame dataSetUS = LoadData(
        new[]
        {
            new object[] {3, "ManufacturerD", "US"}, new object[] {4, "ManufacturerE", "US"},
            new object[] {5, "ManufacturerF", "US"}
        });

    DataFrame dataSetCN = LoadData(
        new[] { new object[] { 6, "ManufacturerG", "CN" }, new object[] { 7, "ManufacturerH", "CN" }, });

    // We initialize a new check for the following data fields
    var check = new Check(CheckLevel.Warning, "generic check")
        .IsComplete("manufacturerName")
        .ContainsURL("manufacturerName", val => val == 0.0)
        .IsContainedIn("countryCode", new[] { "DE", "US", "CN" });

    // We create a new Analysis instance with the corresponding RequiredAnalyzers defined in the check
    Analysis analysis = new Analysis(check.RequiredAnalyzers());
```


Once we have the dataset and the constraints in place, we can trigger the execution of the resulting analyzers for each `partitionKey` using the `AnalysisRunner`.
Because we need to persist the internal state of the computations, we need a `InMemoryStateProvider` instance for each `partitionKey`:

```c#
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
```

The above implementation run the analysis for each partition key by persisting the state in the corresponding `InMemoryStateProvider`.
The implementation proceeds by aggregating the already-computed states using the `AnalyzsisRunner.RunOnAggregatedStates` method.
This operation doesn't need any re-computation on the data since the final metrics were already computed independently.

Let us now assume that a single `partitionKey` changes and that we need to recompute the metrics for the table as a whole.
The stateful computation allows to recompute only the state of the changed partition in order to update the metrics for the whole table.


```c#
// Lets now assume that a single partition changes. We only need to recompute the state of this
// partition in order to update the metrics for the whole table.
DataFrame updatedUsManufacturers = LoadData(new[]
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
```

An [executable version of this example](IncrementalMetrics.cs) is part of our codebase.

