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
    /// A state representing the min value for a column.
    /// </summary>
    public sealed class MinState : DoubleValuedState<MinState>
    {
        /// <summary>
        /// The min value returned by the state.
        /// </summary>
        private readonly double _minValue;

        /// <summary>
        /// Initializes a new instance of type <see cref="MinState"/>.
        /// </summary>
        /// <param name="minValue">The max value returned by the state.</param>
        public MinState(double minValue) => _minValue = minValue;

        /// <inheritdoc cref="DoubleValuedState{S}.Sum"/>
        public override MinState Sum(MinState other) => new MinState(Math.Min(_minValue, other._minValue));

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue() => _minValue;
    }
    /// <summary>
    /// Computes the min value for the target column.
    /// </summary>
    public class Minimum : StandardScanShareableAnalyzer<MinState>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Minimum"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public Minimum(string column, Option<string> where) : base("Minimum", column, MetricEntity.Column, column, where)
        {
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions() =>
            new[] { Min(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double") };

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        protected override Option<MinState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MinState(result.GetAs<double>(offset)));

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };
    }
}
