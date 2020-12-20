using System;
using System.Collections.Generic;
using deequ.Analyzers.States;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    /// <summary>
    /// A state representing the max value for a column.
    /// </summary>
    public class MaxState : DoubleValuedState<MaxState>
    {
        /// <summary>
        /// The max value returned by the state.
        /// </summary>
        private readonly double _maxValue;

        /// <summary>
        /// Initializes a new instance of type <see cref="MaxState"/>.
        /// </summary>
        /// <param name="maxValue">The max value returned by the state.</param>
        public MaxState(double maxValue) => _maxValue = maxValue;

        /// <inheritdoc cref="DoubleValuedState{S}.Sum"/>
        public override MaxState Sum(MaxState other) => new MaxState(Math.Max(_maxValue, other._maxValue));

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue() => _maxValue;
    }

    /// <summary>
    /// Computes the max value for the target column.
    /// </summary>
    public sealed class Maximum : StandardScanShareableAnalyzer<MaxState>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Maximum"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public Maximum(string column, Option<string> where) : base("Maximum", column, MetricEntity.Column,
            column, where)
        {
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions() =>
            new[] { Max(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double") };

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        protected override Option<MaxState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MaxState(result.GetAs<double>(offset)));

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };
    }
}
