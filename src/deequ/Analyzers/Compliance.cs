using System.Collections.Generic;
using System.Text;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;


namespace deequ.Analyzers
{
    public sealed class Compliance : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        public readonly Column Predicate;
        public readonly Option<string> Where;

        public Compliance(string instance, Column predicate, Option<string> where) : base("Compliance", instance,
            MetricEntity.Column)
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

        protected override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () =>
                new NumMatchesAndCount(result.GetAs<int>(offset), result.GetAs<int>(offset + 1)), 2);


        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Instance)
                .Append(",")
                .Append(Predicate)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
