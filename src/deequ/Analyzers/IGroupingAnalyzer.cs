using System.Collections.Generic;

namespace deequ.Analyzers
{
    public interface IGroupingAnalyzer<out M> : IAnalyzer<M>
    {
        public IEnumerable<string> GroupingColumns();
    }
}
