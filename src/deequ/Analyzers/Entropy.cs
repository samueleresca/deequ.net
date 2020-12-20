using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using deequ.Extensions;
using deequ.Util;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;


namespace deequ.Analyzers
{
    /// <summary>
    /// Entropy is a measure of the level of information contained in a message. Given the probability
    /// distribution over values in a column, it describes how many bits are required to identify a value.
    /// </summary>
    public sealed class Entropy : ScanShareableFrequencyBasedAnalyzer
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Entropy"/> class.
        /// </summary>
        /// <param name="column">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public Entropy(Option<string> column, Option<string> where) : base("Entropy",
            new[] { column.Value }.AsEnumerable(), where)
        {
        }
        /// <summary>
        /// Initializes a new instance of type <see cref="Entropy"/> class.
        /// </summary>
        /// <param name="column">The target column names subject to the grouping.</param>
        public Entropy(Option<string> column) : base("Entropy", new[] { column.Value }.AsEnumerable())
        {
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            Func<Column, Column> summands = Udf<double, double>(count =>
            {
                if (count == 0.0)
                {
                    return 0.0;
                }

                return -(count / numRows) * Math.Log(count / numRows);
            });

            return new[] { Sum(summands(Col(AnalyzersExt.COUNT_COL).Cast("double"))) };
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.ToString"/>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Columns.First())
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
