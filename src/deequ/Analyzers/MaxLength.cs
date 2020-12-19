using System;
using System.Collections.Generic;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    /// <summary>
    /// Computes the max value for the target column.
    /// </summary>
    public sealed class MaxLength : StandardScanShareableAnalyzer<MaxState>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="MaxLength"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MaxLength(string column, Option<string> where)
            : base("MaxLength", column, MetricEntity.Column, column, where)
        {
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions() => new[]
        {
            Max(Length(AnalyzersExt.ConditionalSelection(Column, Where))).Cast("double")
        };

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        protected override Option<MaxState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MaxState(result.GetAs<double>(offset)));

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsString(Column.GetOrElse(string.Empty)) };

    }
}
