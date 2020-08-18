using System;
using System.Collections.Generic;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers
{
    /// <summary>
    /// Common interface for all analyzers which generates metrics from states computed on data frames
    /// </summary>
    /// <typeparam name="M">The metric associated with the analyzer <see cref="IMetric"/></typeparam>
    public interface IAnalyzer<out M> where M : IMetric
    {
        /// <summary>
        /// Runs the preconditions, calculation and returns the metric resulting from the analyzer.
        /// </summary>
        /// <param name="data">The data to run the calculation on</param>
        /// <param name="aggregateWith">The provider to load the previously computed state that can be combined with the current dataset</param>
        /// <param name="saveStateWith">The provider to save the state of the computation.</param>
        /// <returns></returns>
        public M Calculate(DataFrame data, Option<IStateLoader> aggregateWith = default, Option<IStatePersister> saveStateWith = default);
        /// <summary>
        /// A set of assertions that must hold on the schema of the data frame
        /// </summary>
        /// <returns>The list of preconditions</returns>
        public IEnumerable<Action<StructType>> Preconditions();
        /// <summary>
        /// A method that wraps the exception into a metric instance <see cref="IMetric"/>
        /// </summary>
        /// <param name="e">The exception to wrap.</param>
        /// <returns></returns>
        public M ToFailureMetric(Exception e);
        /// <summary>
        /// Aggregates two states loaded by the state loader and saves the resulting aggregation using the target state
        /// persister.
        /// </summary>
        /// <param name="sourceA">The first state to load <see cref="IStateLoader"/></param>
        /// <param name="sourceB">The second state to load <see cref="IStateLoader"/></param>
        /// <param name="target">The target persister <see cref="IStatePersister"/></param>
        public void AggregateStateTo(IStateLoader sourceA, IStateLoader sourceB, IStatePersister target);
        /// <summary>
        /// Load the state and returns the computed metric.
        /// </summary>
        /// <param name="source">The state loader <see cref="IStateLoader"/></param>
        /// <returns>The resulting metric</returns>
        public M LoadStateAndComputeMetric(IStateLoader source);
        /// <summary>
        /// Copy and persist the state from a loader.
        /// </summary>
        /// <param name="source">The loader to use <see cref="IStateLoader"/></param>
        /// <param name="target">The persister to use <see cref="IStatePersister"/></param>
        public void CopyStateTo(IStateLoader source, IStatePersister target);
    }
}
