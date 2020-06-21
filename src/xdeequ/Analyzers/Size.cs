using System;
using System.Collections.Generic;
using System.Linq;
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
        public NumMatches(long numMatches)
        {
            this.numMatches = numMatches;
        }

        private long numMatches { get; }

        public IState Sum(IState other)
        {
            var specific = (NumMatches)other;
            return new NumMatches(numMatches + specific.numMatches);
        }

        public override NumMatches Sum(NumMatches other)
        {
            return new NumMatches(numMatches + other.numMatches);
        }

        public override double MetricValue()
        {
            return numMatches;
        }
    }

    public class Size : StandardScanShareableAnalyzer<NumMatches>, IFilterableAnalyzer
    {
        public readonly Option<string> Where;

        public Size(Option<string> where) : base("Size", "*", Entity.DataSet)
        {
            Where = where;
        }

        private Size() : base("Size", "*", Entity.DataSet)
        {
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            return new[] { AnalyzersExt.ConditionalCount(Where) }.AsEnumerable();
        }

        public override Option<NumMatches> FromAggregationResult(Row result, int offset)
        {
            return AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatches(result.GetAs<int>(offset)));
        }

        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return Enumerable.Empty<Action<StructType>>();
        }

        public Option<string> FilterCondition() => Where;
    }
}