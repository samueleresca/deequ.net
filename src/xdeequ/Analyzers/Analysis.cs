using System.Collections.Generic;
using System.Linq;
using xdeequ.Metrics;

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


    }
}