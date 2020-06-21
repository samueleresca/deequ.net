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
    public class Completeness : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer

    {
        public readonly Option<string> Where;
        public readonly Option<string> Column;

        public Completeness(Option<string> column, Option<string> where) : base("Completeness", column.Value,
            Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public Completeness(Option<string> column) : base("Completeness", column.Value, Entity.Column)
        {
            Column = column;
            Where = Option<string>.None;
        }

        public Option<string> FilterCondition()
        {
            return Where;
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
        {
            return AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatchesAndCount(
                    result.GetAs<int>(offset),
                    result.GetAs<int>(offset + 1)), 2);
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            var summarization = Sum(AnalyzersExt.ConditionalSelection(Column, Where)
                .IsNotNull()
                .Cast("int"));

            var conditional = AnalyzersExt.ConditionalCount(Where);

            return new[] { summarization, conditional };
        }

        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[]
            {
                AnalyzersExt.HasColumn(Column.Value),
                AnalyzersExt.IsNotNested(Column.Value)
            };
        }
    }
}