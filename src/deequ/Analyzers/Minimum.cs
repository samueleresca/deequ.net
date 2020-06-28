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
    public class MinState : DoubleValuedState<MinState>, IState
    {
        private readonly double _minValue;

        public MinState(double minValue) => _minValue = minValue;

        public IState Sum(IState other)
        {
            MinState otherMin = (MinState)other;
            return new MinState(Math.Min(_minValue, otherMin._minValue));
        }

        public override MinState Sum(MinState other) => new MinState(Math.Min(_minValue, other._minValue));

        public override double MetricValue() => _minValue;
    }

    public class Minimum : StandardScanShareableAnalyzer<MinState>, IFilterableAnalyzer
    {
        public string Column;
        public Option<string> Where;


        public Minimum(string column, Option<string> where) : base("Minimum", column, Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public Option<string> FilterCondition() => Where;


        public override IEnumerable<Column> AggregationFunctions() =>
            new[] {Min(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double")};

        public override Option<MinState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MinState(result.GetAs<double>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] {AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column)};
    }
}
