using System;
using System.Collections.Generic;
using System.Text;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    public sealed class MaxLength : StandardScanShareableAnalyzer<MaxState>, IFilterableAnalyzer
    {
        public MaxLength(string column, Option<string> where)
            : base("MaxLength", column, MetricEntity.Column, column, where)
        {
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions() => new[]
        {
            Max(Length(AnalyzersExt.ConditionalSelection(Column, Where))).Cast("double")
        };

        protected override Option<MaxState> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () => new MaxState(result.GetAs<double>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsString(Column.GetOrElse(string.Empty)) };

    }
}
