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
    public class MinState : DoubleValuedState<MinState>
    {
        private double _minValue;

        public MinState(double minValue)
        {
            _minValue = minValue;
        }

        public override MinState Sum(MinState other)
        {
            return new MinState(Math.Min(_minValue, other._minValue));
        }

        public override double MetricValue()
        {
            return _minValue;
        }
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

        public static Minimum Create(string column)
        {
            return new Minimum(column, new Option<string>());
        }

        public static Minimum Create(string column, string where)
        {
            return new Minimum(column, where);
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            return new[] {Min(AnalyzersExt.ConditionalSelection(Column, Where)).Cast("double")};
        }

        public override Option<MinState> FromAggregationResult(Row result, int offset)
        {
            return AnalyzersExt.IfNoNullsIn(result, offset, () => new MinState(result.GetAs<double>(offset)));
        }

        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[] {AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNumeric(Column)};
        }

        public Option<string> FilterCondition() => Where;
    }
}