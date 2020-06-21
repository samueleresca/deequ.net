using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public class Correlation : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer,
        IAnalyzer<DoubleMetric>
    {
        public Correlation(string name, string instance, Entity entity) : base(name, instance, entity)
        {
        }

        public Option<string> FilterCondition() => throw new NotImplementedException();

        public override IEnumerable<Column> AggregationFunctions() => throw new NotImplementedException();

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            throw new NotImplementedException();
    }
}
