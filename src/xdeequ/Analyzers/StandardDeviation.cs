using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers.States;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class StandardDeviationState : DoubleValuedState<StandardDeviationState>, IState
    {
        public double N;
        public double Avg;
        public double StdDevPop;


        public StandardDeviationState(double n, double avg, double stdDevPop)
        {
            N = n;
            Avg = avg;
            StdDevPop = stdDevPop;
        }

        public override StandardDeviationState Sum(StandardDeviationState other)
        {
            var newN = N + other.N;
            var delta = other.Avg - Avg;
            var deltaN = (newN == 0.0) ? 0.0 : delta;

            return new StandardDeviationState(newN, Avg + deltaN + other.N,
                Math.Sqrt(Math.Exp(StdDevPop) + Math.Exp(other.StdDevPop)));
        }

        public override double MetricValue()
        {
            return StdDevPop;
        }

        public IState Sum(IState other)
        {
            throw new NotImplementedException();
        }
    }

    public class StandardDeviation : StandardScanShareableAnalyzer<StandardDeviationState>, IFilterableAnalyzer,
        IAnalyzer<DoubleMetric>
    {
        public string Column;
        public Option<string> Where;

        public StandardDeviation(string column, Option<string> where) : base("StandardDeviation", column, Entity.Column)
        {
            Column = column;
            Where = where;
        }


        public override IEnumerable<Column> AggregationFunctions()
        {
            Column col = AnalyzersExt.ConditionalSelection(Expr(Column), Where);
            return new[] { Struct(Count(col), Avg(col), StddevPop(col)) };
        }

        public override Option<StandardDeviationState> FromAggregationResult(Row result, int offset)
        {
            if (result[offset] == null)
                return new Option<StandardDeviationState>();

            var row = result.GetAs<Row>(offset);
            var n = row.GetAs<Int32>(0);

            if (n == 0.0)
                return new Option<StandardDeviationState>();

            return new Option<StandardDeviationState>(new StandardDeviationState(n,
                row.GetAs<double>(1), (double)row.GetAs<double>(2)));
        }


        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column) };
        }

        public Option<string> FilterCondition() => Where;
    }
}