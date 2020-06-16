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

        public Analysis(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {
            Analyzers = analyzers;
        }

        public Analysis AddAnalyzer(IAnalyzer<IMetric> analyzer)
        {
            return new Analysis(Analyzers.Append(analyzer));
        }

        public Analysis AddAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {
            return new Analysis(Analyzers.Concat(analyzers));
        }

        public AnalyzerContext Run(DataFrame data,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateWith,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses)
        {
            return AnalysisRunner.DoAnalysisRun(
                data,
                Analyzers, aggregateWith, saveStateWith, storageLevelOfGroupedDataForMultiplePasses);
        }
    }
}