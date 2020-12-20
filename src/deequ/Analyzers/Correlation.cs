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
    ///  Computes the pearson correlation coefficient between the two given columns
    /// </summary>
    public class Correlation : StandardScanShareableAnalyzer<CorrelationState>, IFilterableAnalyzer
    {
        /// <summary>
        /// First input column for computation.
        /// </summary>
        public readonly string ColumnA;
        /// <summary>
        /// Second input column for computation.
        /// </summary>
        public readonly string ColumnB;

        /// <summary>
        /// Initializes a new instance of the <see cref="Correlation"/> class.
        /// </summary>
        /// <param name="columnA">First input column for computation</param>
        /// <param name="columnB">Second input column for computation.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public Correlation(string columnA, string columnB, Option<string> where = default) : base("Correlation",
            $"{columnA},{columnB}", MetricEntity.Multicolumn, Option<string>.None, where)
        {
            ColumnA = columnA;
            ColumnB = columnB;
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.AggregationFunctions" />
        public override IEnumerable<Column> AggregationFunctions()
        {
            //https://mathoverflow.net/a/57914
            var firstSelection = AnalyzersExt.ConditionalSelection(ColumnA, Where);
            var secondSelection = AnalyzersExt.ConditionalSelection(ColumnB, Where);

            var count = Count(firstSelection);
            var sumX = Sum(firstSelection);
            var sumY = Sum(secondSelection);
            var sumXY = Sum(firstSelection * secondSelection);
            var sumX2 = Sum(firstSelection * firstSelection);
            var sumY2 = Sum(secondSelection * secondSelection);

            //double n, double sumX, double sumY, double sumXY, double sumX2, double sumY2
            return new[] { count, sumX, sumY, sumXY, sumX2, sumY2 };
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.FromAggregationResult" />
        protected override Option<CorrelationState> FromAggregationResult(Row result, int offset)
        {
            if (result[offset] == null)
            {
                return Option<CorrelationState>.None;
            }
            var n = result.GetAs<Int32>(0);

            if (n > 0.0)
            {
                return new CorrelationState(
                    n,
                    result.GetAs<Int32>(1),
                    result.GetAs<Int32>(2),
                    result.GetAs<Int32>(3),
                    result.GetAs<Int32>(4),
                    result.GetAs<Int32>(5)
                );
            }

            return Option<CorrelationState>.None;
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions" />
        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[]
            {
                AnalyzersExt.HasColumn(ColumnA), AnalyzersExt.IsNumeric(ColumnA),
                AnalyzersExt.HasColumn(ColumnB), AnalyzersExt.IsNumeric(ColumnB)
            };
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.ToString"/>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(ColumnA)
                .Append(",")
                .Append(ColumnB)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }

    /// <summary>
    /// Describes the correlation state.
    /// </summary>
    public class CorrelationState : DoubleValuedState<CorrelationState>
    {

        private double n;
        private double sumX;
        private double sumX2;
        private double sumY;
        private double sumY2;
        private double sumXY;


        /// <summary>
        /// Initializes a new instance of the <see cref="CorrelationState"/> class.
        /// </summary>
        /// <param name="n">Number of records.</param>
        /// <param name="sumX">Sum of the first column.</param>
        /// <param name="sumY">Sum of the second column.</param>
        /// <param name="sumXY">Sum of the multiplication between the columns.</param>
        /// <param name="sumX2">Sum of each value of the first column ^2.</param>
        /// <param name="sumY2">Sum of each value of the second column ^2.</param>
        public CorrelationState(double n, double sumX, double sumY, double sumXY, double sumX2, double sumY2)
        {
            this.n = n;
            this.sumX = sumX;
            this.sumY = sumY;
            this.sumXY = sumXY;
            this.sumX2 = sumX2;
            this.sumY2 = sumY2;
        }

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue()
        {
            return (n * sumXY - (sumX * sumY)) /
                   (Math.Sqrt(n * sumX2 - Math.Pow(sumX, 2)) * Math.Sqrt(n * sumY2 - Math.Pow(sumY, 2)));
        }

        /// <inheritdoc cref="State{T}.Sum"/>
        public override CorrelationState Sum(CorrelationState other)
        {
            double n1 = n;
            double n2 = other.n;
            double newN = n1 + n2;

            double newSumX = sumX + other.sumX;
            double newSumY = sumY + other.sumY;
            double newSumXY = sumXY + other.sumXY;
            double newSumX2 = sumX2 + other.sumX2;
            double newSumY2 = sumY2 + other.sumY2;

            return new CorrelationState(newN, newSumX, newSumY, newSumXY, newSumX2, newSumY2);
        }

        /// <summary>
        /// Overrides the equality between objects.
        /// </summary>
        /// <param name="obj">The object to compare.</param>
        /// <returns>true if the objects are equals, otherwise false.</returns>
        public override bool Equals(object obj)
        {
            if (!(obj is CorrelationState))
            {
                return false;
            }

            var other = (CorrelationState)obj;

            return n == other.n &&
                   sumX == other.sumX &&
                               sumY == other.sumY &&
                                             sumXY == other.sumXY &&
                                                            sumX2 == other.sumX2 &&
                                                                           sumY2 == other.sumY2;
        }
    }
}
