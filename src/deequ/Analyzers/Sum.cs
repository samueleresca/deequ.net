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
    public class SumState : DoubleValuedState<SumState>, IState
    {
        private readonly double _sum;

        public SumState(double sum) => _sum = sum;

        public IState Sum(IState other)
        {
            SumState sumStateOther = (SumState)other;
            return new SumState(_sum + sumStateOther._sum);
        }

        public override SumState Sum(SumState other) => new SumState(_sum + other._sum);

        public override double GetMetricValue() => _sum;
    }

    public sealed class Sum : StandardScanShareableAnalyzer<SumState>, IFilterableAnalyzer
    {
        public Sum(string column, Option<string> where) : base("Sum", column, MetricEntity.Column, column, where)
        {
        }

        public DoubleMetric Calculate(DataFrame data) => base.Calculate(data);

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions() =>
            new[] { Sum(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double") };

        protected override Option<SumState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new SumState(result.GetAs<double>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };
    }
}
