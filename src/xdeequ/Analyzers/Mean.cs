using System;
using System.Collections.Generic;
using Apache.Arrow.Types;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers.States;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;
using StructType = Microsoft.Spark.Sql.Types.StructType;

namespace xdeequ.Analyzers
{
    public class MeanState : DoubleValuedState<MeanState>
    {
        private double _sum;
        private long _count;

        public MeanState(double sum, long count)
        {
            _sum = sum;
            _count = count;
        }

        public override MeanState Sum(MeanState other)
        {
            return new MeanState(_sum + other._sum, _count + other._count);
        }

        public override double MetricValue()
        {
            if (_count == 0L) return double.NaN;
            return _sum / _count;
        }
    }

    public class Mean : StandardScanShareableAnalyzer<MeanState>, IFilterableAnalyzer
    {
        public string Column;
        public Option<string> Where;


        public Mean(string column, Option<string> where) : base("Mean", column, Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            return new[]
            {
                Sum(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double"),
                Count(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("long")
            };
        }

        public override Option<MeanState> FromAggregationResult(Row result, int offset)
        {
            return AnalyzersExt.IfNoNullsIn(result, offset,
                () => new MeanState((double)result.Get(offset),
                    (int)result.Get(offset + 1)), 2);
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column) };
        }
    }
}