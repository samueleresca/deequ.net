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
    /// Completeness computes the fraction of non-null values ina column of a <see cref="DataFrame"/>
    /// </summary>
    public sealed class Completeness : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Completeness"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Functions.Expr"/>.</param>
        public Completeness(Option<string> column, Option<string> where) : base("Completeness", column.Value,
            MetricEntity.Column, column, where)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Completeness"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        public Completeness(Option<string> column) : base("Completeness", column.Value, MetricEntity.Column)
        {
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.FromAggregationResult"/>
        protected override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatchesAndCount(
                    result.GetAs<int>(offset),
                    result.GetAs<int>(offset + 1)), 2);

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions()
        {
            Column summarization = Sum(AnalyzersExt.ConditionalSelection(Column, Where)
                .IsNotNull()
                .Cast("int"));

            Column conditional = AnalyzersExt.ConditionalCount(Where);

            return new[] { summarization, conditional };
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNotNested(Column) };

    }
}
