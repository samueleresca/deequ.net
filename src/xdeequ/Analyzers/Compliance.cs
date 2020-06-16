using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class Compliance : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer,
        IAnalyzer<DoubleMetric>
    {
        private readonly Column _predicate;
        private readonly Option<string> _where;

        public Compliance(string instance, Column predicate, Option<string> where) : base("Compliance", instance,
            Entity.Column)
        {
            _where = where;
            _predicate = predicate;
        }

        public Option<string> FilterCondition()
        {
            return _where;
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            var summation = Sum(AnalyzersExt.ConditionalSelection(_predicate, _where).Cast("int"));

            return new[] {summation, AnalyzersExt.ConditionalCount(_where)};
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
        {
            return AnalyzersExt.IfNoNullsIn(result, offset, () =>
                new NumMatchesAndCount(result.GetAs<int>(offset), result.GetAs<int>(offset + 1)), 2);
        }
    }
}