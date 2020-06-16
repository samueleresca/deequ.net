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
        private readonly Option<string> _where;
        private readonly Option<string> _column;

        public Completeness(Option<string> column, Option<string> where) : base("Completeness", column.Value,
            Entity.Column)
        {
            _column = column;
            _where = where;
        }

        public Completeness(Option<string> column) : base("Completeness", column.Value, Entity.Column)
        {
            _column = column;
            _where = Option<string>.None;
        }

        public Option<string> FilterCondition()
        {
            return _where;
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
            var summarization = Sum(AnalyzersExt.ConditionalSelection(_column, _where)
                .IsNotNull()
                .Cast("int"));

            var conditional = AnalyzersExt.ConditionalCount(_where);

            return new[] { summarization, conditional };
        }

        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[]
            {
                AnalyzersExt.HasColumn(_column.Value),
                AnalyzersExt.IsNotNested(_column.Value)
            };
        }
    }
}