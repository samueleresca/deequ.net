using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers.States;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public class NumMatches : DoubleValuedState<NumMatches>, IState
    {
        public NumMatches(long numMatches) => this.numMatches = numMatches;

        private long numMatches { get; }

        public IState Sum(IState other)
        {
            NumMatches specific = (NumMatches)other;
            return new NumMatches(numMatches + specific.numMatches);
        }

        public override NumMatches Sum(NumMatches other) => new NumMatches(numMatches + other.numMatches);

        public override double MetricValue() => numMatches;
    }

    public sealed class Size : StandardScanShareableAnalyzer<NumMatches>, IFilterableAnalyzer
    {
        public readonly Option<string> Where;

        public Size(Option<string> where) : base("Size", "*", Entity.Dataset) => Where = where;

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions() =>
            new[] {AnalyzersExt.ConditionalCount(Where)}.AsEnumerable();

        public override Option<NumMatches> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatches(result.GetAs<int>(offset)));

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            Enumerable.Empty<Action<StructType>>();

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
