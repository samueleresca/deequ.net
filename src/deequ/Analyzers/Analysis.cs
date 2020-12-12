using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers.Runners;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using StorageLevel = deequ.Analyzers.Runners.StorageLevel;

namespace deequ.Analyzers
{
    public class Analysis
    {
        public IEnumerable<IAnalyzer<IMetric>> Analyzers;

        public Analysis(IEnumerable<IAnalyzer<IMetric>> analyzers) => Analyzers = analyzers;

        public Analysis() => Analyzers = new List<IAnalyzer<IMetric>>();

        public Analysis AddAnalyzer(IAnalyzer<IMetric> analyzer) => new Analysis(Analyzers.Append(analyzer));

        public Analysis AddAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers) =>
            new Analysis(Analyzers.Concat(analyzers));

        public AnalyzerContext Run(DataFrame data,
            Option<IStateLoader> aggregateWith = default,
            Option<IStatePersister> saveStateWith = default,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK) =>
            AnalysisRunner.DoAnalysisRun(
                data,
                Analyzers, aggregateWith, saveStateWith, storageLevelOfGroupedDataForMultiplePasses);
    }
}
