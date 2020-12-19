using System;
using System.Collections.Generic;
using deequ.Analyzers.States;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    public sealed class MinState : DoubleValuedState<MinState>
    {
        private readonly double _minValue;

        public MinState(double minValue) => _minValue = minValue;

        public override MinState Sum(MinState other) => new MinState(Math.Min(_minValue, other._minValue));

        public override double GetMetricValue() => _minValue;
    }

    public class Minimum : StandardScanShareableAnalyzer<MinState>, IFilterableAnalyzer
    {
        public Minimum(string column, Option<string> where) : base("Minimum", column, MetricEntity.Column, column, where)
        {
        }

        public Option<string> FilterCondition() => Where;


        public override IEnumerable<Column> AggregationFunctions() =>
            new[] { Min(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double") };

        protected override Option<MinState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MinState(result.GetAs<double>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column.GetOrElse(string.Empty)) };
    }
}
