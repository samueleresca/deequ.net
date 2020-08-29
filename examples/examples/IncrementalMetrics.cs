using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Checks;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace examples
{
    public static class IncrementalMetrics
    {
        public static void IncrementalChangesOnManufacturers()
        {
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
                new[] {new object[] {6, "ManufacturerG", "CN"}, new object[] {7, "ManufacturerH", "CN"},});

            // We initialize a new check for the following data fields
            var check = new Check(CheckLevel.Warning, "generic check")
                .IsComplete("manufacturerName")
                .ContainsURL("manufacturerName", val => val == 0.0)
                .IsContainedIn("countryCode", new[] {"DE", "US", "CN"});


            // We create a new Analysis instance with the corresponding RequiredAnalyzers defined in the check
            Analysis analysis = new Analysis(check.RequiredAnalyzers());

            // We create a new in-memory state provider for each countryCode defined in the dataset
            InMemoryStateProvider deStates = new InMemoryStateProvider();
            InMemoryStateProvider usStates = new InMemoryStateProvider();
            InMemoryStateProvider cnStates = new InMemoryStateProvider();

            // These call will store the resulting metrics in the separate states providers for each dataSet
            AnalysisRunner.Run(dataSetDE, analysis, deStates);
            AnalysisRunner.Run(dataSetUS, analysis, usStates);
            AnalysisRunner.Run(dataSetCN, analysis, cnStates);

            // Next, we are able to compute the metrics for the whole table from the partition states
            // This just aggregates the previously calculated metrics, it doesn't performs computation on the data
            AnalyzerContext tableMetrics = AnalysisRunner.RunOnAggregatedStates(dataSetDE.Schema(), analysis,
                new[] {deStates, usStates, cnStates});

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
                new[] {deStates, usStates, cnStates});
        }

        private static DataFrame LoadData(IEnumerable<object[]> rows) =>
            SparkSession.Builder().GetOrCreate().CreateDataFrame(
                rows.Select(row => new GenericRow(row)),
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()),
                    new StructField("manufacturerName", new StringType()),
                    new StructField("countryCode", new StringType())
                }));
    }
}
