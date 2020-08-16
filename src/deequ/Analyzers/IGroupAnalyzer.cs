using System.Collections.Generic;

namespace deequ.Analyzers
{
    public interface IGroupAnalyzer<S, out M> : IAnalyzer<M>
    {
        public IEnumerable<string> GroupingColumns();
    }
}
