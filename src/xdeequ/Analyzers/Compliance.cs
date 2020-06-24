using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class Compliance : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        public readonly Column Predicate;
        public readonly Option<string> Where;

        public Compliance(string instance, Column predicate, Option<string> where) : base("Compliance", instance,
            Entity.Column)
        {
            Where = where;
            Predicate = predicate;
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions()
        {
            Column summation = Sum(AnalyzersExt.ConditionalSelection(Predicate, Where).Cast("int"));

            return new[] { summation, AnalyzersExt.ConditionalCount(Where) };
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () =>
                new NumMatchesAndCount(result.GetAs<int>(offset), result.GetAs<int>(offset + 1)), 2);
    }
}
