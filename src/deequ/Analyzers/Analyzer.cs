using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using deequ.Analyzers.States;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;


namespace deequ.Analyzers
{
    /// <summary>
    /// A abstract class that represents all the analyzers which generates metrics from states computed on data frames.
    /// </summary>
    /// <typeparam name="S">The input <see cref="State{T}"/> of the analyzer.</typeparam>
    /// <typeparam name="M">The output <see cref="Metric{T}"/> of the analyzer.</typeparam>
    public abstract class Analyzer<S, M> : IAnalyzer<M> where S : State<S> where M : IMetric
    {
        /// <summary>
        /// Wraps an <see cref="Exception"/> in an <see cref="IMetric"/> instance.
        /// </summary>
        /// <param name="e">The <see cref="Exception"/> to wrap into a metric</param>
        /// <returns>The <see cref="Metric{T}"/> instance.</returns>
        public abstract M ToFailureMetric(Exception e);

        /// <summary>
        /// A set of assertions that must hold on the schema of the data frame.
        /// </summary>
        /// <returns>The list of preconditions.</returns>
        public virtual IEnumerable<Action<StructType>> Preconditions() => Enumerable.Empty<Action<StructType>>();

        /// <summary>
        /// Runs preconditions, calculates and returns the metric
        /// </summary>
        /// <param name="data">The <see cref="DataFrame"/> being analyzed.</param>
        /// <param name="aggregateWith">The <see cref="IStateLoader"/> for previous states to include in the computation.</param>
        /// <param name="saveStateWith">The <see cref="IStatePersister"/>loader for previous states to include in the computation. </param>
        /// <returns>Returns the failure <see cref="Metric{T}"/> in case of the preconditions fail.</returns>
        public virtual M Calculate(DataFrame data, Option<IStateLoader> aggregateWith = default, Option<IStatePersister> saveStateWith = default)
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

        /// <summary>
        /// Aggregates two states loaded by the state loader and saves the resulting aggregation using the target state
        /// persister.
        /// </summary>
        /// <param name="sourceA">The first state to load <see cref="IStateLoader"/></param>
        /// <param name="sourceB">The second state to load <see cref="IStateLoader"/></param>
        /// <param name="target">The target persister <see cref="IStatePersister"/></param>
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

        /// <summary>
        /// Load the <see cref="State{T}"/> from a <see cref="IStateLoader"/> and compute the <see cref="Metric{T}"/>
        /// </summary>
        /// <param name="source">The <see cref="IStateLoader"/>.</param>
        /// <returns>Returns the computed <see cref="Metric{T}"/>.</returns>
        public M LoadStateAndComputeMetric(IStateLoader source) =>
            ComputeMetricFrom(source.Load<S>(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this)));

        /// <summary>
        /// Copy the state from source to target.
        /// </summary>
        /// <param name="source">The <see cref="IStateLoader"/> to read from.</param>
        /// <param name="target">The <see cref="IStatePersister"/> to write to.</param>
        public void CopyStateTo(IStateLoader source, IStatePersister target) =>
            target.Persist(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this), source.Load<S>(new Option<IAnalyzer<IMetric>>((IAnalyzer<IMetric>)this)));

        /// <summary>
        /// Compute the state (sufficient statistics) from the data.
        /// </summary>
        /// <param name="dataFrame">The <see cref="DataFrame"/> to compute state from.</param>
        /// <returns>The computed <see cref="State{T}"/>.</returns>
        public abstract Option<S> ComputeStateFrom(DataFrame dataFrame);

        /// <summary>
        /// Compute the metric from the state (sufficient statistics)
        /// </summary>
        /// <param name="state">The <see cref="State{T}"/> to compute metrics from.</param>
        /// <returns>The computed <see cref="Metric{T}"/>.</returns>
        public abstract M ComputeMetricFrom(Option<S> state);

        /// <summary>
        /// Calculate a <see cref="Metric{T}"/> from a <see cref="State{T}"/>
        /// </summary>
        /// <param name="state">The <see cref="State{T}"/> to compute metrics from.</param>
        /// <param name="aggregateWith">The <see cref="IStateLoader"/> for previous states to include in the computation.</param>
        /// <param name="saveStateWith">The <see cref="IStatePersister"/>loader for previous states to include in the computation. </param>
        /// <returns></returns>
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

    /// <summary>
    /// An analyzer that runs a set of aggregation functions over the data, can share scans over the data.
    /// </summary>
    /// <typeparam name="S">The input <see cref="State{T}"/> of the analyzer.</typeparam>
    /// <typeparam name="M">The output <see cref="Metric{T}"/> of the analyzer.</typeparam>
    public abstract class ScanShareableAnalyzer<S, M> : Analyzer<S, M>, IScanSharableAnalyzer<S, M>
        where S : State<S>, IState where M : IMetric
    {
        /// <summary>
        /// The target column name subject to the aggregation.
        /// </summary>
        public Option<string> Column;

        /// <summary>
        /// A where clause to filter only some values in a column <see cref="Expr"/>.
        /// </summary>
        public Option<string> Where;

        /// <summary>
        /// Defines the aggregations to compute on the data.
        /// </summary>
        /// <returns>The <see cref="IEnumerable{T}"/> of type <see cref="Column"/> containing the aggregation function.</returns>
        public abstract IEnumerable<Column> AggregationFunctions();

        /// <summary>
        /// Computes the state from the result of the aggregation functions and calculate the <see cref="Metric{T}"/>.
        /// </summary>
        /// <param name="result"></param>
        /// <param name="offset"></param>
        /// <param name="aggregateWith"></param>
        /// <param name="saveStatesWith"></param>
        /// <returns>Returns the computed <see cref="Metric{M}"/>.</returns>
        public M MetricFromAggregationResult(Row result, int offset, Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStatesWith)
        {
            Option<S> state = FromAggregationResult(result, offset);
            return CalculateMetric(state, aggregateWith, saveStatesWith);
        }

        /// <summary>
        /// Computes the state from the result of the aggregation functions.
        /// </summary>
        /// <param name="result">The result of the aggregation function as a data frame <see cref="Row"/>.</param>
        /// <param name="offset">The offset of the cell containing the aggregation function.</param>
        /// <returns>Returns the resulting <see cref="State{T}"/>.</returns>
        protected abstract Option<S> FromAggregationResult(Row result, int offset);

        /// <inheritdoc cref="Analyzer{S,M}.ComputeStateFrom"/>
        public override Option<S> ComputeStateFrom(DataFrame dataFrame)
        {
            IEnumerable<Column> aggregations = AggregationFunctions();
            Row result = dataFrame
                .Agg(aggregations.First(), aggregations.Skip(1).ToArray())
                .Collect()
                .FirstOrDefault();

            return FromAggregationResult(result, 0);
        }

        /// <summary>
        /// Retrieve the filter condition assigned to the instance
        /// </summary>
        /// <returns>The filter condition assigned to the instance</returns>
        public virtual Option<string> FilterCondition() => Where;

        /// <summary>
        /// Overrides the ToString method.
        /// </summary>
        /// <returns>Returns the string identifier of the analyzer in the following format: AnalyzerType(column_name, where).</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Column)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
    /// <summary>
    /// A scan-shareable analyzer that produces a <see cref="DoubleMetric"/> instance.
    /// </summary>
    /// <typeparam name="S">The input <see cref="DoubleValuedState{S}"/> of the analyzer.</typeparam>
    public abstract class StandardScanShareableAnalyzer<S> : ScanShareableAnalyzer<S, DoubleMetric>,
        IScanSharableAnalyzer<IState, DoubleMetric>
        where S : DoubleValuedState<S>
    {
        /// <summary>
        /// The <see cref="MetricEntity"/> that represents the analyzer.
        /// </summary>
        private readonly MetricEntity _metricEntity;

        /// <summary>
        /// The name of the analyzer.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// A string representing the instance of the analyzer.
        /// </summary>
        public string Instance { get; }

        /// <summary>
        /// Initializes a new instance of type <see cref="StandardScanShareableAnalyzer{S}"/>.
        /// </summary>
        /// <param name="name">The name of the analyzer.</param>
        /// <param name="instance">The instance of the metric.</param>
        /// <param name="metricEntity">A string representing the instance of the analyzer.</param>
        protected StandardScanShareableAnalyzer(string name, string instance, MetricEntity metricEntity)
        {
            Name = name;
            Instance = instance;
            _metricEntity = metricEntity;
        }
        /// <summary>
        /// Initializes a new instance of type <see cref="StandardScanShareableAnalyzer{S}"/>.
        /// </summary>
        /// <param name="name">The name of the metric.</param>
        /// <param name="instance">The instance of the metric.</param>
        /// <param name="metricEntity">The entity type of the metric <see cref="MetricEntity"/></param>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        protected StandardScanShareableAnalyzer(string name, string instance, MetricEntity metricEntity,
            Option<string> column = default, Option<string> where = default)
        {
            Name = name;
            Instance = instance;
            Column = column;
            Where = where;

            _metricEntity = metricEntity;
        }

        /// <inheritdoc cref="Analyzer{S,M}.Preconditions"/>
        public override IEnumerable<Action<StructType>> Preconditions() =>
            AdditionalPreconditions().Concat(base.Preconditions());

        /// <inheritdoc cref="Analyzer{S,M}.ToFailureMetric"/>
        public override DoubleMetric ToFailureMetric(Exception e) =>
            AnalyzersExt.MetricFromFailure(e, Name, Instance, _metricEntity);

        /// <summary>
        /// A set of additional assertions that must hold on the schema of the data frame.
        /// </summary>
        /// <returns>The list of preconditions.</returns>
        public virtual IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            Enumerable.Empty<Action<StructType>>();

        /// <inheritdoc cref="Analyzer{S,M}.ComputeMetricFrom"/>
        public override DoubleMetric ComputeMetricFrom(Option<S> state)
        {
            DoubleMetric metric = state.HasValue switch
            {
                true => AnalyzersExt.MetricFromValue(new Try<double>(state.Value.GetMetricValue()), Name, Instance,
                    _metricEntity),
                _ => AnalyzersExt.MetricFromEmpty(this, Name, Instance, _metricEntity)
            };

            return metric;
        }
    }

    /// <summary>
    /// A state for computing ratio-based metrics, contains a N rows that match a predicate on overall of TOT rows.
    /// </summary>
    public class NumMatchesAndCount : DoubleValuedState<NumMatchesAndCount>
    {
        /// <summary>
        /// Total number of rows.
        /// </summary>
        public long Count;

        /// <summary>
        /// Total number of matches.
        /// </summary>
        public long NumMatches;

        /// <summary>
        /// Initializes a new instance of type <see cref="NumMatchesAndCount"/>.
        /// </summary>
        /// <param name="numMatches">Total number of matches.</param>
        /// <param name="count">Total number of rows.</param>
        public NumMatchesAndCount(long numMatches, long count)
        {
            NumMatches = numMatches;
            Count = count;
        }

        /// <inheritdoc cref="State{S}.Sum"/>
        public override NumMatchesAndCount Sum(NumMatchesAndCount other) =>
            new NumMatchesAndCount(NumMatches + other.NumMatches, Count + other.Count);

        /// <inheritdoc cref="DoubleValuedState{S}.GetMetricValue"/>
        public override double GetMetricValue()
        {
            if (Count == 0L)
            {
                return double.NaN;
            }

            return (double)NumMatches / Count;
        }
    }

    /// <summary>
    /// Base class for analyzers that require to group the data by specific columns.
    /// </summary>
    /// <typeparam name="S">The input <see cref="State{T}"/> of the analyzer.</typeparam>
    /// <typeparam name="M">The output <see cref="Metric{T}"/> of the analyzer.</typeparam>
    public abstract class GroupingAnalyzer<S, M> : Analyzer<S, M>, IGroupingAnalyzer<M>, IFilterableAnalyzer where S : State<S> where M : IMetric
    {
        /// <summary>
        /// The name of the grouping analyzer.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The target column names subject to the grouping.
        /// </summary>
        public IEnumerable<string> Columns { get; protected set; }
        /// <summary>
        /// A where clause to filter only some values in a column <see cref="Expr"/>.
        /// </summary>
        public Option<string> Where { get; set; }

        /// <summary>
        /// Initializes a new instance of type <see cref="GroupingAnalyzer{S,M}"/> class.
        /// </summary>
        /// <param name="name">The name of the grouping analyzer.</param>
        /// <param name="columns">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        protected GroupingAnalyzer(string name, IEnumerable<string> columns, Option<string> where)
        {
            Name = name;
            Columns = columns;
            Where = where;
        }

        /// <inheritdoc cref="IGroupingAnalyzer{M}.GroupingColumns"/>
        public abstract IEnumerable<string> GroupingColumns();

        /// <inheritdoc cref="Analyzer{S,M}.Preconditions"/>
        public override IEnumerable<Action<StructType>> Preconditions() =>
            GroupingColumns().Select(HasColumn).Concat(base.Preconditions());

        /// <summary>
        /// Checks if a schema has a specific column name.
        /// </summary>
        /// <param name="column">The name of the column to verify.</param>
        /// <returns>A callback asserting the presence of the column in the schema.</returns>
        private static Action<StructType> HasColumn(string column) =>
            schema =>
            {
                if (!AnalyzersExt.HasColumn(schema, column))
                {
                    throw new Exception("Input data does not include column!");
                }
            };

        /// <summary>
        /// Retrieve the filter condition assigned to the instance
        /// </summary>
        /// <returns>The filter condition assigned to the instance</returns>
        public virtual Option<string> FilterCondition() => Where;
    }

    internal abstract class PredicateMatchingAnalyzer : StandardScanShareableAnalyzer<NumMatchesAndCount>
    {
        public Column Predicate { get; }

        protected PredicateMatchingAnalyzer(string name, string instance, MetricEntity metricEntity,
            Column predicate, Option<string> where) : base(name, instance, metricEntity)
        {
            Predicate = predicate;
            Where = where;
        }

        protected override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
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


}
