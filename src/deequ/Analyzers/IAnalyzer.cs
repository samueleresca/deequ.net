using System;
using System.Collections.Generic;
using deequ.Analyzers.States;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers
{
    /// <summary>
    /// A abstract class that represents all the analyzers which generates metrics from states computed on data frames.
    /// </summary>
    /// <typeparam name="S">The input <see cref="State{T}"/> of the analyzer.</typeparam>
    /// <typeparam name="M">The output <see cref="Metric{T}"/> of the analyzer.</typeparam>
    public interface IAnalyzer<out M> where M : IMetric
    {
        /// <summary>
        /// Runs preconditions, calculates and returns the metric
        /// </summary>
        /// <param name="data">The <see cref="DataFrame"/> being analyzed.</param>
        /// <param name="aggregateWith">The <see cref="IStateLoader"/> for previous states to include in the computation.</param>
        /// <param name="saveStateWith">The <see cref="IStatePersister"/>loader for previous states to include in the computation. </param>
        /// <returns>Returns the failure <see cref="Metric{T}"/> in case of the preconditions fail.</returns>
        M Calculate(DataFrame data, Option<IStateLoader> aggregateWith = default, Option<IStatePersister> saveStateWith = default);

        /// <summary>
        /// A set of assertions that must hold on the schema of the data frame.
        /// </summary>
        /// <returns>The list of preconditions.</returns>
        IEnumerable<Action<StructType>> Preconditions();

        /// <summary>
        /// Wraps an <see cref="Exception"/> in an <see cref="IMetric"/> instance.
        /// </summary>
        /// <param name="e">The <see cref="Exception"/> to wrap into a metric</param>
        /// <returns>The <see cref="Metric{T}"/> instance.</returns>
        M ToFailureMetric(Exception e);

        /// <summary>
        /// Aggregates two states loaded by the state loader and saves the resulting aggregation using the target state
        /// persister.
        /// </summary>
        /// <param name="sourceA">The first state to load <see cref="IStateLoader"/></param>
        /// <param name="sourceB">The second state to load <see cref="IStateLoader"/></param>
        /// <param name="target">The target persister <see cref="IStatePersister"/></param>
        private void AggregateStateTo(IStateLoader sourceA, IStateLoader sourceB, IStatePersister target)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Load the <see cref="State{T}"/> from a <see cref="IStateLoader"/> and compute the <see cref="Metric{T}"/>
        /// </summary>
        /// <param name="source">The <see cref="IStateLoader"/>.</param>
        /// <returns>Returns the computed <see cref="Metric{T}"/>.</returns>
        M LoadStateAndComputeMetric(IStateLoader source);

        /// <summary>
        /// Copy the state from source to target.
        /// </summary>
        /// <param name="source">The <see cref="IStateLoader"/> to read from.</param>
        /// <param name="target">The <see cref="IStatePersister"/> to write to.</param>
        void CopyStateTo(IStateLoader source, IStatePersister target);
    }
}
