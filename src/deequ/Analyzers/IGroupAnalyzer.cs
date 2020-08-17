using System.Collections.Generic;

namespace deequ.Analyzers
{
    public interface IGroupAnalyzer<out M> : IAnalyzer<M>
    {
        public IEnumerable<string> GroupingColumns();
    }
}
