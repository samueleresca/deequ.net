using System.Collections.Generic;
using xdeequ.Analyzers;

namespace xdeequ.Metrics
{
    public interface IMetricRepositoryMultipleResultsLoader
    {
        public IMetricRepositoryMultipleResultsLoader WithTagValues(Dictionary<string, string> tagValues);
        public IMetricRepositoryMultipleResultsLoader ForAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers);
        public IMetricRepositoryMultipleResultsLoader After(long dateTime);
        public IMetricRepositoryMultipleResultsLoader Before(long dateTime);
        public IEnumerable<AnalysisResult> Get();
    }
}
