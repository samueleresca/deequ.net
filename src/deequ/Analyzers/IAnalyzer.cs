using System;
using System.Collections.Generic;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers
{
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
