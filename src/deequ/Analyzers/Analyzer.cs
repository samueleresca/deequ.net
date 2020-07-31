using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers.States;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public interface IAnalyzer<out M>
    {
        public M Calculate(DataFrame data);
        public M Calculate(DataFrame data, Option<IStateLoader> aggregateWith, Option<IStatePersister> saveStateWith);
        public IEnumerable<Action<StructType>> Preconditions();
        public M ToFailureMetric(Exception e);
        public void AggregateStateTo(IStateLoader sourceA, IStateLoader sourceB, IStatePersister target);
        public M LoadStateAndComputeMetric(IStateLoader source);
        public void CopyStateTo(IStateLoader source, IStatePersister target);
    }

    public interface IGroupAnalyzer<S, out M> : IAnalyzer<M>
    {
        public IEnumerable<string> GroupingColumns();
    }

    public interface IScanSharableAnalyzer<S, out M> : IAnalyzer<M>
    {
        public IEnumerable<Column> AggregationFunctions();

        public M MetricFromAggregationResult(Row result, int offset, Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStatesWith);

        public new M Calculate(DataFrame data, Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateWith);
    }

    public abstract class Analyzer<S, M> : IAnalyzer<M> where S : State<S>
    {
        public abstract M ToFailureMetric(Exception e);

        public virtual IEnumerable<Action<StructType>> Preconditions() => Enumerable.Empty<Action<StructType>>();

        public M Calculate(DataFrame data, Option<IStateLoader> aggregateWith, Option<IStatePersister> saveStateWith)
        {
            try
            {
                foreach (Action<StructType> condition in Preconditions())
                {
                    condition(data.Schema());
                }

                Option<S> state = ComputeStateFrom(data);
                return CalculateMetric(state, aggregateWith, saveStateWith);
            }
            catch (Exception e)
            {
                return ToFailureMetric(e);
            }
        }

        public M Calculate(DataFrame data) => Calculate(data, Option<IStateLoader>.None, Option<IStatePersister>.None);

        public void AggregateStateTo(IStateLoader sourceA, IStateLoader sourceB, IStatePersister target)
        {
            Option<S> maybeStateA = sourceA.Load<S>(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this));
            Option<S> maybeStateB = sourceB.Load<S>(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this));

            S aggregated = (maybeStateA.HasValue, maybeStateB.HasValue) switch
            {
                (true, true) => maybeStateA.Value.Sum(maybeStateB.Value),
                (true, false) => maybeStateA.Value,
                (false, true) => maybeStateB.Value,
                _ => null
            };

            target.Persist(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this), new Option<S>(aggregated));
        }

        public M LoadStateAndComputeMetric(IStateLoader source) =>
            ComputeMetricFrom(source.Load<S>(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this)));

        public void CopyStateTo(IStateLoader source, IStatePersister target) =>
            target.Persist(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this), source.Load<S>(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this)));

        public abstract Option<S> ComputeStateFrom(DataFrame dataFrame);
        public abstract M ComputeMetricFrom(Option<S> state);

        public M CalculateMetric(Option<S> state, Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateWith)
        {
            Option<S> loadedState = aggregateWith
                .Select(value => value.Load<S>(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this)).Value);


            Option<S> stateToComputeMetricFrom = AnalyzersExt.Merge(loadedState, state);

            saveStateWith
                .Select(persister =>
                    persister.Persist(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this), stateToComputeMetricFrom));

            return ComputeMetricFrom(stateToComputeMetricFrom);
        }
    }

    public abstract class ScanShareableAnalyzer<S, M> : Analyzer<S, M>, IScanSharableAnalyzer<S, M>
        where S : State<S>, IState
    {
        public abstract IEnumerable<Column> AggregationFunctions();

        public M MetricFromAggregationResult(Row result, int offset, Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStatesWith)
        {
            Option<S> state = FromAggregationResult(result, offset);
            return CalculateMetric(state, aggregateWith, saveStatesWith);
        }

        public abstract Option<S> FromAggregationResult(Row result, int offset);

        public override Option<S> ComputeStateFrom(DataFrame dataFrame)
        {
            IEnumerable<Column> aggregations = AggregationFunctions();
            Row result = dataFrame
                .Agg(aggregations.First(), aggregations.Skip(1).ToArray())
                .Collect()
                .FirstOrDefault();

            return FromAggregationResult(result, 0);
        }
    }

    internal abstract class StandardScanShareableAnalyzer<S> : ScanShareableAnalyzer<S, DoubleMetric>,
        IScanSharableAnalyzer<IState, DoubleMetric>
        where S : DoubleValuedState<S>, IState
    {
        public Entity Entity = Entity.Column;

        public StandardScanShareableAnalyzer(string name, string instance, Entity entity)
        {
            Name = name;
            Instance = instance;
            Entity = entity;
        }

        public string Name { get; set; }
        public string Instance { get; set; }

        public override IEnumerable<Action<StructType>> Preconditions() =>
            AdditionalPreconditions().Concat(base.Preconditions());

        public override DoubleMetric ToFailureMetric(Exception e) =>
            AnalyzersExt.MetricFromFailure(e, Name, Instance, Entity);

        public virtual IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            Enumerable.Empty<Action<StructType>>();

        public override DoubleMetric ComputeMetricFrom(Option<S> state)
        {
            DoubleMetric metric = state.HasValue switch
            {
                true => AnalyzersExt.MetricFromValue(new Try<double>(state.Value.MetricValue()), Name, Instance,
                    Entity),
                _ => AnalyzersExt.MetricFromEmpty(this, Name, Instance, Entity)
            };

            return metric;
        }
    }


    public class NumMatchesAndCount : DoubleValuedState<NumMatchesAndCount>, IState
    {
        public long Count;
        public long NumMatches;

        public NumMatchesAndCount(long numMatches, long count)
        {
            NumMatches = numMatches;
            Count = count;
        }

        public IState Sum(IState other) => throw new NotImplementedException();

        public override NumMatchesAndCount Sum(NumMatchesAndCount other) =>
            new NumMatchesAndCount(NumMatches + other.NumMatches, Count + other.Count);

        public override double MetricValue()
        {
            if (Count == 0L)
            {
                return double.NaN;
            }

            return (double)NumMatches / Count;
        }
    }

    internal abstract class PredicateMatchingAnalyzer : StandardScanShareableAnalyzer<NumMatchesAndCount>
    {
        protected PredicateMatchingAnalyzer(string name, string instance, Entity entity,
            Column predicate, Option<string> where) : base(name, instance, entity)
        {
            Predicate = predicate;
            Where = where;
        }

        public Column Predicate { get; set; }
        public Option<string> Where { get; set; }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
        {
            if (result[offset] == null || result[offset + 1] == null)
            {
                return Option<NumMatchesAndCount>.None;
            }

            NumMatchesAndCount state = new NumMatchesAndCount((long)result[offset], (long)result[offset + 1]);
            return new Option<NumMatchesAndCount>(state);
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            Column selection = AnalyzersExt.ConditionalSelection(Predicate, Where);
            return new[] { selection, Count("*") }.AsEnumerable();
        }
    }

    public abstract class GroupingAnalyzer<S, M> : Analyzer<S, M>, IGroupAnalyzer<IState, M> where S : State<S>
    {
        public abstract IEnumerable<string> GroupingColumns();

        public new M Calculate(DataFrame data) => base.Calculate(data);

        public override IEnumerable<Action<StructType>> Preconditions() =>
            GroupingColumns().Select(HasColumn).Concat(base.Preconditions());


        public static Action<StructType> HasColumn(string column) =>
            schema =>
            {
                if (!AnalyzersExt.HasColumn(schema, column))
                {
                    throw new Exception("Input data does not include column!");
                }
            };
    }
}
