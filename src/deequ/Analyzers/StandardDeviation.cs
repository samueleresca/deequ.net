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
    public class StandardDeviationState : DoubleValuedState<StandardDeviationState>
    {
        public double Avg;
        public double N;
        public double StdDevPop;


        public StandardDeviationState(double n, double avg, double stdDevPop)
        {
            N = n;
            Avg = avg;
            StdDevPop = stdDevPop;
        }

        public override StandardDeviationState Sum(StandardDeviationState other)
        {
            double newN = N + other.N;
            double delta = other.Avg - Avg;
            double deltaN = newN == 0.0 ? 0.0 : delta;

            return new StandardDeviationState(newN, Avg + deltaN + other.N,
                Math.Sqrt(Math.Exp(StdDevPop) + Math.Exp(other.StdDevPop)));
        }

        public override double GetMetricValue() => StdDevPop;
    }

    public sealed class StandardDeviation : StandardScanShareableAnalyzer<StandardDeviationState>
    {
        public StandardDeviation(string column, Option<string> where) : base("StandardDeviation", column, MetricEntity.Column, column, where)
        {
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            Column col = AnalyzersExt.ConditionalSelection(Expr(Column.GetOrElse(string.Empty)), Where);
            return new[] { Struct(Count(col), Avg(col), StddevPop(col)) };
        }

        protected override Option<StandardDeviationState> FromAggregationResult(Row result, int offset)
        {
            if (result[offset] == null)
            {
                return new Option<StandardDeviationState>();
            }

            Row row = result.GetAs<Row>(offset);
            int n = row.GetAs<int>(0);

            if (n == 0.0)
            {
                return new Option<StandardDeviationState>();
            }

            return new Option<StandardDeviationState>(new StandardDeviationState(n,
                row.GetAs<double>(1), row.GetAs<double>(2)));
        }


        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };

    }
}
