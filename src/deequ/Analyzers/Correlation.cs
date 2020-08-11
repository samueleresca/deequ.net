using System;
using System.Collections.Generic;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers
{
    internal class Correlation : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
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
