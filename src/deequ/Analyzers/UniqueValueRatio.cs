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
    /// Computes the unique value ratio of a <see cref="DataFrame"/> column.
    /// </summary>
    public class UniqueValueRatio : ScanShareableFrequencyBasedAnalyzer
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="UniqueValueRatio"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public UniqueValueRatio(IEnumerable<string> columns, Option<string> where) : base("UniqueValueRatio", columns, where)
        {
        }

        /// <summary>
        /// Initializes a new instance of type <see cref="UniqueValueRatio"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        public UniqueValueRatio(IEnumerable<string> columns) : base("UniqueValueRatio", columns)
        {
        }

        /// <inheritdoc cref="ScanShareableFrequencyBasedAnalyzer.ToFailureMetric"/>
        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        /// <inheritdoc cref="ScanShareableFrequencyBasedAnalyzer.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] { Sum(Col(AnalyzersExt.COUNT_COL).EqualTo(Lit(1)).Cast("double")), Count("*") };

        /// <inheritdoc cref="ScanShareableFrequencyBasedAnalyzer.FromAggregationResult"/>
        public override DoubleMetric FromAggregationResult(Row result, int offset)
        {
            double numUniqueValues = (double)result[offset];
            int numDistinctValues = (int)result[offset + 1];

            return ToSuccessMetric(numUniqueValues / numDistinctValues);
        }
    }
}
