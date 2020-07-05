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
                analyzerContext.MetricMap.Where(x => x.Value.IsSuccess());

            AnalyzerContext analyzerContextWithSuccessfulValues =
                new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(successfulMetrics));

            _resultsRepository[resultKey] = new AnalysisResult(resultKey, analyzerContextWithSuccessfulValues);
        }

        public Option<AnalyzerContext> LoadByKey(ResultKey resultKey)
        {
            return !_resultsRepository.ContainsKey(resultKey) ? Option<AnalyzerContext>.None : new Option<AnalyzerContext>(_resultsRepository[resultKey]?.AnalyzerContext);
        }

        public IMetricRepositoryMultipleResultsLoader Load() =>
            new LimitedInMemoryMetricsRepositoryMultipleResultsLoader(_resultsRepository);
    }

    public class LimitedInMemoryMetricsRepositoryMultipleResultsLoader : MetricsRepositoryMultipleResultsLoader
    {
        private readonly ConcurrentDictionary<ResultKey, AnalysisResult> _resultsRepository;
        private Option<long> after;
        private Option<long> before;
        private Option<IEnumerable<IAnalyzer<IMetric>>> forAnalyzers;
        private Option<Dictionary<string, string>> tagValues;

        public LimitedInMemoryMetricsRepositoryMultipleResultsLoader(
            ConcurrentDictionary<ResultKey, AnalysisResult> resultsRepository) =>
            _resultsRepository = resultsRepository;

        public override IMetricRepositoryMultipleResultsLoader WithTagValues(Dictionary<string, string> tagValues)
        {
            this.tagValues = new Option<Dictionary<string, string>>(tagValues);
            return this;
        }

        public override IMetricRepositoryMultipleResultsLoader ForAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {
            forAnalyzers = new Option<IEnumerable<IAnalyzer<IMetric>>>(analyzers);
            return this;
        }

        public override IMetricRepositoryMultipleResultsLoader After(long dateTime)
        {
            after = new Option<long>(dateTime);
            return this;
        }

        public override IMetricRepositoryMultipleResultsLoader Before(long dateTime)
        {
            before = new Option<long>(dateTime);
            return this;
        }

        public override IEnumerable<AnalysisResult> Get() =>
            _resultsRepository
                .Where(pair => !after.HasValue || after.Value <= pair.Key.DataSetDate)
                .Where(pair => !before.HasValue || pair.Key.DataSetDate <= before.Value)
                .Where(pair => !tagValues.HasValue || pair.Key.Tags.Any(x =>
                    tagValues.Value.TryGetValue(x.Key, out string found) && found == x.Value))
                .Select(x =>
                {
                    IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> requestedMetrics = x.Value
                        .AnalyzerContext
                        .MetricMap
                        .Where(analyzer => !forAnalyzers.HasValue || forAnalyzers.Value.Contains(analyzer.Key));

                    return new AnalysisResult(x.Value.ResultKey,
                        new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(requestedMetrics)));
                });
    }
}
