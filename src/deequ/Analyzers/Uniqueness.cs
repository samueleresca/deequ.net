using System;
using System.Collections.Generic;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    /// <summary>
    /// Uniqueness is the fraction of unique values of a column(s), i.e., values that occur exactly once.
    /// </summary>
    public sealed class Uniqueness : ScanShareableFrequencyBasedAnalyzer
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Uniqueness"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public Uniqueness(IEnumerable<string> columns, Option<string> where) : base("Uniqueness", columns, where)
        {
        }

        /// <summary>
        /// Initializes a new instance of type <see cref="Uniqueness"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        public Uniqueness(IEnumerable<string> columns) : base("Uniqueness", columns)
        {
        }

        /// <inheritdoc cref="ScanShareableFrequencyBasedAnalyzer.ToFailureMetric"/>
        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        /// <inheritdoc cref="ScanShareableFrequencyBasedAnalyzer.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] { Sum(Col(AnalyzersExt.COUNT_COL).EqualTo(Lit(1)).Cast("double")) / numRows };
    }
}
