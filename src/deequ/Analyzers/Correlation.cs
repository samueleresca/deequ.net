using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers.States;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    internal class Correlation : StandardScanShareableAnalyzer<CorrelationState>, IFilterableAnalyzer
    {
        private string firstCol;
        private string secondCol;
        private Option<string> where;

        public Correlation(string firstCol, string secondCol, Option<string> where) : base("Correlation",
            $"{firstCol},{secondCol}", Entity.Multicolumn)
        {
            this.firstCol = firstCol;
            this.secondCol = secondCol;
            this.where = where;
        }

        public Option<string> FilterCondition() => throw new NotImplementedException();

        public override IEnumerable<Column> AggregationFunctions()
        {
            //https://mathoverflow.net/a/57914
            var firstSelection = AnalyzersExt.ConditionalSelection(firstCol, where);
            var secondSelection = AnalyzersExt.ConditionalSelection(secondCol, where);


            var count = Count(firstSelection);
            var sumX = Sum(firstSelection);
            var sumY = Sum(secondSelection);
            var meanX = sumX / count;
            var meanY = sumY / count;
            var correlation = Corr(firstSelection, secondSelection);
            return Enumerable.Empty<Column>();
        }

        public override Option<CorrelationState> FromAggregationResult(Row result, int offset)
        {
            if (result[offset] == null) {
                return Option<CorrelationState>.None;
            }

            var row = result.GetAs<Row>(offset);
            var n = row.GetAs<double>(0);

            if (n > 0.0)
            {
                return new CorrelationState(
                    n,
                    row.GetAs<double>(1),
                    row.GetAs<double>(2),
                    row.GetAs<double>(3),
                    row.GetAs<double>(4),
                    row.GetAs<double>(5)
                );
            }

            return Option<CorrelationState>.None;
        }
    }

    internal class CorrelationState : DoubleValuedState<CorrelationState>
    {

        private double n;
        private double xAvg;
        private double yAvg;
        private double ck;
        private double xMk;
        private double yMk;

        public CorrelationState(double n, double xAvg, double yAvg, double ck, double xMk, double yMk)
        {
            this.n = n;
            this.xAvg = xAvg;
            this.yAvg = yAvg;
            this.ck = ck;
            this.xMk = xMk;
            this.yMk = yMk;
        }

        public override double MetricValue()
        {
            return ck / Math.Sqrt(xMk * yMk);
        }
    }
}
