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
    public class MinLength : StandardScanShareableAnalyzer<MinState>, IFilterableAnalyzer, IAnalyzer<DoubleMetric>
    {
        public string Column;
        public Option<string> Where;


        public MinLength(string column, Option<string> where) : base("MinLength", column, Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions() => new[]
        {
            Min(Length(AnalyzersExt.ConditionalSelection(Column, Where))).Cast("double")
        };

        public override Option<MinState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MinState(result.GetAs<double>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] {AnalyzersExt.HasColumn(Column), AnalyzersExt.IsString(Column)};
    }
}
