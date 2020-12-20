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
    /// Encapsulates a sum state.
    /// </summary>
    public class SumState : DoubleValuedState<SumState>
    {
        /// <summary>
        /// The sum value for the data.
        /// </summary>
        private readonly double _sum;

        /// <summary>
        /// Initializes a new instance of type <see cref="SumState"/> class.
        /// </summary>
        /// <param name="sum">The sum value for the data.</param>
        public SumState(double sum) => _sum = sum;

        /// <inheritdoc cref="DoubleValuedState{S}.Sum"/>
        public override SumState Sum(SumState other) => new SumState(_sum + other._sum);

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue() => _sum;
    }

    /// <summary>
    /// Computes the sum of data.
    /// </summary>
    public sealed class Sum : StandardScanShareableAnalyzer<SumState>
    {
        /// <summary>
        ///  Initializes a new instance of type <see cref="Sum"/> class.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="where"></param>
        public Sum(string column, Option<string> where) : base("Sum", column, MetricEntity.Column, column, where)
        {
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.Calculate"/>
        public DoubleMetric Calculate(DataFrame data) => base.Calculate(data);

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions() =>
            new[] { Sum(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double") };

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        protected override Option<SumState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new SumState(result.GetAs<double>(offset)));

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };
    }
}
