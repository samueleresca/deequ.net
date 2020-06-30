using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class MaxLength : StandardScanShareableAnalyzer<MaxState>, IFilterableAnalyzer, IAnalyzer<DoubleMetric>
    {
        public string Column;
        public Option<string> Where;

        public MaxLength(string column, Option<string> where) : base("MaxLength", column, Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions() => new[]
        {
            Max(Length(AnalyzersExt.ConditionalSelection(Column, Where))).Cast("double")
        };

        public override Option<MaxState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MaxState(result.GetAs<double>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] {AnalyzersExt.HasColumn(Column), AnalyzersExt.IsString(Column)};
    }
}
