using deequ.Analyzers.Runners;
using deequ.Util;

namespace deequ.Repository
{
    public interface IMetricsRepository
    {
        public void Save(ResultKey resultKey, AnalyzerContext analyzerContext);
        public Option<AnalyzerContext> LoadByKey(ResultKey resultKey);
        public IMetricRepositoryMultipleResultsLoader Load();
    }
}
