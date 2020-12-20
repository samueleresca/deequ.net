using System;
using System.Collections.Generic;
using System.Text;
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
    /// Represents the mean result state for the target column.
    /// </summary>
    public class MeanState : DoubleValuedState<MeanState>
    {
        /// <summary>
        /// The total count of elements.
        /// </summary>
        private readonly long _count;
        /// <summary>
        /// The sum of all the elements.
        /// </summary>
        private readonly double _sum;

        /// <summary>
        /// Initializes a new instance of type <see cref="MeanState"/>.
        /// </summary>
        /// <param name="sum">The sum of all the elements.</param>
        /// <param name="count">The total count of elements.</param>
        public MeanState(double sum, long count)
        {
            _sum = sum;
            _count = count;
        }

        /// <inheritdoc cref="DoubleValuedState{S}.Sum"/>
        public override MeanState Sum(MeanState other) => new MeanState(_sum + other._sum, _count + other._count);

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue()
        {
            if (_count == 0L)
            {
                return double.NaN;
            }

            return _sum / _count;
        }
    }
    /// <summary>
    /// Computes the mean for the target column.
    /// </summary>
    public sealed class Mean : StandardScanShareableAnalyzer<MeanState>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Mean"/>.
        /// </summary>
        /// <param name="column"></param>
        /// <param name="where"></param>
        public Mean(string column, Option<string> where) : base("Mean", column, MetricEntity.Column, column, where)
        {
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions() =>
            new[]
            {
                Sum(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double"),
                Count(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("long")
            };

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.FromAggregationResult"/>
        protected override Option<MeanState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new MeanState((double)result.Get(offset),
                    (int)result.Get(offset + 1)), 2);

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };

    }
}
