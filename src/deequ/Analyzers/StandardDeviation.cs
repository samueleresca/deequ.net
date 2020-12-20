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
    /// A type representing the standard deviation state.
    /// </summary>
    public class StandardDeviationState : DoubleValuedState<StandardDeviationState>
    {
        /// <summary>
        /// The average of the state.
        /// </summary>
        private readonly double Avg;
        /// <summary>
        /// The number of elements in the sequence.
        /// </summary>
        private readonly double N;
        /// <summary>
        /// The M squared coefficient.
        /// </summary>
        private readonly double StdDev;


        /// <summary>
        /// Initializes a new instance of type <see cref="StandardDeviationState"/> class.
        /// </summary>
        /// <param name="n">The number of elements in the sequence.</param>
        /// <param name="avg">The average of the state.</param>
        /// <param name="stdDev">The M squared coefficient.</param>
        public StandardDeviationState(double n, double avg, double stdDev)
        {
            N = n;
            Avg = avg;
            StdDev = stdDev;
        }


        /// <inheritdoc cref="State{S}.Sum"/>
        public override StandardDeviationState Sum(StandardDeviationState other)
        {
            double newN = N + other.N;
            double delta = other.Avg - Avg;
            double deltaN = newN == 0.0 ? 0.0 : delta;

            return new StandardDeviationState(newN, Avg + deltaN + other.N,
                Math.Sqrt(Math.Exp(StdDev) + Math.Exp(other.StdDev)));
        }

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue() => StdDev;
    }

    /// <summary>
    /// Computes the standard deviation of a column.
    /// </summary>
    public sealed class StandardDeviation : StandardScanShareableAnalyzer<StandardDeviationState>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="StandardDeviation"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public StandardDeviation(string column, Option<string> where)
            : base("StandardDeviation", column, MetricEntity.Column, column, where)
        {
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions()
        {
            Column col = AnalyzersExt.ConditionalSelection(Expr(Column.GetOrElse(string.Empty)), Where);
            return new[] { Struct(Count(col), Avg(col), StddevPop(col)) };
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        protected override Option<StandardDeviationState> FromAggregationResult(Row result, int offset)
        {
            if (result[offset] == null)
            {
                return new Option<StandardDeviationState>();
            }

            Row row = result.GetAs<Row>(offset);
            int n = row.GetAs<int>(0);

            if (n == 0.0)
            {
                return new Option<StandardDeviationState>();
            }

            return new Option<StandardDeviationState>(new StandardDeviationState(n,
                row.GetAs<double>(1), row.GetAs<double>(2)));
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };

    }
}
