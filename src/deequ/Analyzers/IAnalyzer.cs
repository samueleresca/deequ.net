using System;
using System.Collections.Generic;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers
{
    /// <summary>
    /// Common interface for all analyzers which generates metrics from states computed on data frames
    /// </summary>
    /// <typeparam name="M">The metric associated with the analyzer <see cref="IMetric"/></typeparam>
    public interface IAnalyzer<out M>
    {

        public M Calculate(DataFrame data, Option<IStateLoader> aggregateWith = default, Option<IStatePersister> saveStateWith = default);
        public IEnumerable<Action<StructType>> Preconditions();
        public M ToFailureMetric(Exception e);
        public void AggregateStateTo(IStateLoader sourceA, IStateLoader sourceB, IStatePersister target);
        public M LoadStateAndComputeMetric(IStateLoader source);
        public void CopyStateTo(IStateLoader source, IStatePersister target);
    }
}
