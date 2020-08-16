using System.Collections.Generic;
using deequ.Util;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers
{
    public interface IScanSharableAnalyzer<S, out M> : IAnalyzer<M>
    {
        public IEnumerable<Column> AggregationFunctions();

        public M MetricFromAggregationResult(Row result, int offset, Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStatesWith);
    }
}
