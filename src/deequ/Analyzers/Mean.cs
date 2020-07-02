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
    public class MeanState : DoubleValuedState<MeanState>, IState
    {
        private readonly long _count;
        private readonly double _sum;

        public MeanState(double sum, long count)
        {
            _sum = sum;
            _count = count;
        }

        public IState Sum(IState other) => throw new NotImplementedException();

        public override MeanState Sum(MeanState other) => new MeanState(_sum + other._sum, _count + other._count);

        public override double MetricValue()
        {
            if (_count == 0L)
            {
                return double.NaN;
            }

            return _sum / _count;
        }
    }

    public sealed class Mean : StandardScanShareableAnalyzer<MeanState>, IFilterableAnalyzer
    {
        public readonly string Column;
        public readonly Option<string> Where;


        public Mean(string column, Option<string> where) : base("Mean", column, Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions() =>
            new[]
            {
                Sum(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double"),
                Count(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("long")
            };

        public override Option<MeanState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new MeanState((double)result.Get(offset),
                    (int)result.Get(offset + 1)), 2);

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column) };

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
