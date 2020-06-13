using System.Collections.Generic;
using System.Linq;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers.Runners
{
    public class AnalyzerContext
    {
        public Dictionary<IAnalyzer<IMetric>, IMetric> MetricMap;

        public static AnalyzerContext Empty() => new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>());

        public AnalyzerContext(Dictionary<IAnalyzer<IMetric>, IMetric> metricMap)
        {
            MetricMap = metricMap;
        }

        public IEnumerable<IMetric> AllMetrics()
        {
            return MetricMap.Values.AsEnumerable();
        }

        public static AnalyzerContext operator +(AnalyzerContext current, AnalyzerContext other)
        {
            current.MetricMap.Merge(other.MetricMap);
            return new AnalyzerContext(current.MetricMap);
        }

        public Option<IMetric> Metric(IAnalyzer<IMetric> analyzer)
        {
            return new Option<IMetric>(MetricMap[analyzer]);
        }

    }
}