using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class Compliance : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        private readonly Option<string> _where;
        private readonly string _predicate;

        public Compliance(string instance, string predicate, Option<string> @where) : base("Compliance", instance,
            Entity.Column)
        {
            _where = where;
            _predicate = predicate;
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            var summation = Sum(AnalyzersExt.ConditionalSelection(Expr(_predicate), _where).Cast("int"));

            return new[] {summation, AnalyzersExt.ConditionalCount(_where)};
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
        {
            return AnalyzersExt.IfNoNullsIn(result, offset, () =>
                new NumMatchesAndCount(result.GetAs<Int32>(offset), result.GetAs<Int32>(offset + 1)), howMany: 2);
        }

        public Option<string> FilterCondition() => _where;
    }
}