using System.Collections.Generic;
using deequ.Analyzers;
using deequ.Metrics;
using Microsoft.Spark.Sql;

namespace deequ.Repository
{
    public interface IMetricRepositoryMultipleResultsLoader
    {
        public IMetricRepositoryMultipleResultsLoader WithTagValues(Dictionary<string, string> tagValues);
        public IMetricRepositoryMultipleResultsLoader ForAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers);
        public IMetricRepositoryMultipleResultsLoader After(long dateTime);
        public IMetricRepositoryMultipleResultsLoader Before(long dateTime);
        public IEnumerable<AnalysisResult> Get();
        public DataFrame GetSuccessMetricsAsDataFrame(SparkSession session, IEnumerable<string> withTags);
        public string GetSuccessMetricsAsJson(IEnumerable<string> withTags);
    }
}
