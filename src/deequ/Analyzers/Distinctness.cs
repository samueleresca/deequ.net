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
    ///  Distinctness is the fraction of distinct values of a column(s).
    /// </summary>
    public sealed class Distinctness : ScanShareableFrequencyBasedAnalyzer
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Distinctness"/> class.
        /// </summary>
        /// <param name="columns">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public Distinctness(IEnumerable<string> columns, Option<string> where) : base("Distinctness", columns, where)
        {
        }

        /// <summary>
        /// Initializes a new instance of type <see cref="Distinctness"/> class.
        /// </summary>
        /// <param name="columns">The target column names subject to the grouping.</param>
        public Distinctness(IEnumerable<string> columns) : base("Distinctness", columns)
        {
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.ToFailureMetric"/>
        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] { Sum(Col(AnalyzersExt.COUNT_COL).Geq(1).Cast("double")) / numRows };

    }
}
