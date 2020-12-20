using System.Collections.Generic;
using deequ.Analyzers.States;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers
{
    /// <summary>
    /// Identifies an analyzer that runs a set of aggregation functions over the data and can share scans over the data.
    /// </summary>
    /// <typeparam name="S">The state used by the analyzer. <see cref="IState"/>></typeparam>
    /// <typeparam name="M">The resulting metric of the analyzer. <see cref="IMetric"/></typeparam>
    public interface IScanSharableAnalyzer<S, out M> : IAnalyzer<M> where M : IMetric
    {
        /// <summary>
        /// Defines the aggregations to compute on the data.
        /// </summary>
        /// <returns>The set of aggregations.</returns>
        public IEnumerable<Column> AggregationFunctions();

        /// <summary>
        /// Produces a metric from the aggregation result
        /// </summary>
        /// <param name="result">The result row to use.</param>
        /// <param name="offset">The offset to use.</param>
        /// <param name="aggregateWith">The loader to use for the aggregation <see cref="IStateLoader"/>.</param>
        /// <param name="saveStatesWith">The persister to use for saving the result <see cref="IStatePersister"/>.</param>
        /// <returns></returns>
        public M MetricFromAggregationResult(Row result, int offset, Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStatesWith);
    }
}
