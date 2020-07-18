using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers.States;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class MaxState : DoubleValuedState<MaxState>, IState
    {
        private readonly double _maxValue;

        public MaxState(double maxValue) => _maxValue = maxValue;

        public IState Sum(IState other)
        {
            MaxState maxState = (MaxState)other;
            return new MaxState(Math.Max(_maxValue, maxState._maxValue));
        }

        public override MaxState Sum(MaxState other) => new MaxState(Math.Max(_maxValue, other._maxValue));

        public override double MetricValue() => _maxValue;
    }

    public sealed class Maximum : StandardScanShareableAnalyzer<MaxState>, IFilterableAnalyzer
    {
        public readonly string Column;
        public readonly Option<string> Where;

        public Maximum(string column, Option<string> where) : base("Maximum", column, Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public Option<string> FilterCondition() => Where;


        public override IEnumerable<Column> AggregationFunctions() =>
            new[] {Max(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double")};

        public override Option<MaxState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MaxState(result.GetAs<double>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] {AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column)};

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Column)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
