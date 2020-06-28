using xdeequ.Analyzers.Runners;
using xdeequ.Util;

namespace xdeequ.Repository
{
    public interface IMetricsRepository
    {
        public void Save(ResultKey resultKey, AnalyzerContext analyzerContext);
        public Option<AnalyzerContext> LoadByKey(ResultKey resultKey);
        public IMetricRepositoryMultipleResultsLoader Load();
    }
}