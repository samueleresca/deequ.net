using System.Collections.Generic;
using deequ.Metrics;

namespace deequ.Analyzers
{
    /// <summary>
    /// Identifies the analyzers that require to group the data by a specific column.
    /// </summary>
    /// <typeparam name="M">The metric of the analyzer <see cref="IMetric"/></typeparam>
    public interface IGroupingAnalyzer<out M> : IAnalyzer<M> where M : IMetric
    {
        /// <summary>
        /// The columns to group the data by
        /// </summary>
        /// <returns>The names of the columns</returns>
        public IEnumerable<string> GroupingColumns();
    }
}
