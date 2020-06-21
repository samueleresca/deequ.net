using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers.Runners;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
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
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateWith,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses) =>
            AnalysisRunner.DoAnalysisRun(
                data,
                Analyzers, aggregateWith, saveStateWith, storageLevelOfGroupedDataForMultiplePasses);
    }
}
