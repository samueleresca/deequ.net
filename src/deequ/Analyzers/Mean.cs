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

        public override double GetMetricValue()
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
        public Mean(string column, Option<string> where) : base("Mean", column, MetricEntity.Column, column, where)
        {
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions() =>
            new[]
            {
                Sum(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double"),
                Count(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("long")
            };

        protected override Option<MeanState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new MeanState((double)result.Get(offset),
                    (int)result.Get(offset + 1)), 2);

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };

    }
}
