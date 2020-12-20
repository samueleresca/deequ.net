using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using deequ.Analyzers.States;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers
{
    /// <summary>
    /// Encapsulate the number of matches.
    /// </summary>
    public class NumMatches : DoubleValuedState<NumMatches>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Size"/> class.
        /// </summary>
        /// <param name="numMatches">The number of matches.</param>
        public NumMatches(long numMatches) => this.numMatches = numMatches;
        /// <summary>
        /// The number of matches.
        /// </summary>
        private long numMatches { get; }

        /// <inheritdoc cref="DoubleValuedState{S}.Sum"/>
        public override NumMatches Sum(NumMatches other) => new NumMatches(numMatches + other.numMatches);

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue() => numMatches;
    }

    /// <summary>
    /// Size computes the number of rows in a <see cref="DataFrame"/>.
    /// </summary>
    public sealed class Size : StandardScanShareableAnalyzer<NumMatches>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Size"/> class.
        /// </summary>
        /// <param name="where">Additional filter to apply before the analyzer is run.</param>
        public Size(Option<string> where) : base("Size", "*", MetricEntity.Dataset, where: where)
        {
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions() =>
            new[] { AnalyzersExt.ConditionalCount(Where) }.AsEnumerable();

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        protected override Option<NumMatches> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatches(result.GetAs<int>(offset)));

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            Enumerable.Empty<Action<StructType>>();

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
