using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Repository.InMemory
{
    public class InMemoryMetricsRepository : IMetricsRepository
    {
        private readonly ConcurrentDictionary<ResultKey, AnalysisResult> _resultsRepository;

        public InMemoryMetricsRepository() =>
            _resultsRepository = new ConcurrentDictionary<ResultKey, AnalysisResult>();

        public void Save(ResultKey resultKey, AnalyzerContext analyzerContext)
        {
            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> successfulMetrics =
                analyzerContext.MetricMap.Where(x => x.Value != null);
            AnalyzerContext analyzerContextWithSuccessfulValues =
                new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(successfulMetrics));
            _resultsRepository[resultKey] = new AnalysisResult(resultKey, analyzerContextWithSuccessfulValues);
        }

        public Option<AnalyzerContext> LoadByKey(ResultKey resultKey) => throw new NotImplementedException();

        public IMetricRepositoryMultipleResultsLoader Load() => throw new NotImplementedException();
    }
}
