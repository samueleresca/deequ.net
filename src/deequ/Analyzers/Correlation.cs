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
    public class Correlation : StandardScanShareableAnalyzer<CorrelationState>, IFilterableAnalyzer
    {
        private string firstCol;
        private string secondCol;
        private Option<string> where;

        public Correlation(string firstCol, string secondCol, Option<string> where = default) : base("Correlation",
            $"{firstCol},{secondCol}", Entity.Multicolumn)
        {
            this.firstCol = firstCol;
            this.secondCol = secondCol;
            this.where = where;
        }

        public Option<string> FilterCondition() => where;

        public override IEnumerable<Column> AggregationFunctions()
        {
            //https://mathoverflow.net/a/57914
            var firstSelection = AnalyzersExt.ConditionalSelection(firstCol, where);
            var secondSelection = AnalyzersExt.ConditionalSelection(secondCol, where);

            var count = Count(firstSelection);
            var sumX2 = Sum(firstSelection * firstSelection);
            var sumY2 = Sum(secondSelection * secondSelection);
            var sumX = Sum(firstSelection);
            var sumY = Sum(secondSelection);

            //double n, double sumX, double sumY, double sumXY, double sumX2, double sumY2
            return new[] {count, sumX, sumY, sumX2, sumY2};
        }

        public override Option<CorrelationState> FromAggregationResult(Row result, int offset)
        {
            if (result[offset] == null) {
                return Option<CorrelationState>.None;
            }
            var n = result.GetAs<Int32>(0);

            if (n > 0.0)
            {
                return new CorrelationState(
                    n,
                    result.GetAs<Int32>(1),
                    result.GetAs<Int32>(2),
                    result.GetAs<Int32>(1) * result.GetAs<Int32>(2),
                    result.GetAs<Int32>(3),
                    result.GetAs<Int32>(4)
                );
            }

            return Option<CorrelationState>.None;
        }

        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[]
            {
                AnalyzersExt.HasColumn(firstCol), AnalyzersExt.IsNumeric(firstCol),
                AnalyzersExt.HasColumn(secondCol), AnalyzersExt.IsNumeric(secondCol)
            };
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(firstCol)
                .Append(",")
                .Append(secondCol)
                .Append(",")
                .Append(where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }

    public class CorrelationState : DoubleValuedState<CorrelationState>
    {

        private double n;
        private double sumX;
        private double sumX2;
        private double sumY;
        private double sumY2;
        private double sumXY;

        public CorrelationState(double n, double sumX, double sumY, double sumXY, double sumX2, double sumY2)
        {
            this.n = n;
            this.sumX = sumX;
            this.sumY = sumY;
            this.sumXY = sumXY;
            this.sumX2 = sumX2;
            this.sumY2 = sumY2;
        }

        public override double MetricValue()
        {
            return n * sumXY - (sumX * sumY);
        }

        public override CorrelationState Sum(CorrelationState other)
        {
            var n1 = n;
            var n2 = other.n;
            var newN = n1 + n2;

            var newSumX = sumX + other.sumX;
            var newSumY = sumY + other.sumY;
            var newSumXY = sumXY + other.sumXY;
            var newSumX2 = sumX2 + other.sumX2;
            var newSumY2 = sumY2 + other.sumY2;

            return new CorrelationState(newN, newSumX, newSumY, newSumXY, newSumX2, newSumY2);
        }
    }
}
