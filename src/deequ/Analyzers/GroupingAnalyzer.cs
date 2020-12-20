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
    /// Base class for all analyzers that compute a (shareable) aggregation over the grouped data.
    /// </summary>
    public abstract class ScanShareableFrequencyBasedAnalyzer : FrequencyBasedAnalyzer
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="ScanShareableFrequencyBasedAnalyzer"/> class.
        /// </summary>
        /// <param name="name">The name of the grouping analyzer.</param>
        /// <param name="columns">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        protected ScanShareableFrequencyBasedAnalyzer(string name, IEnumerable<string> columns, Option<string> where = default)
            : base(name, columns, where)
        {
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.AggregationFunctions"/>
        public abstract IEnumerable<Column> AggregationFunctions(long numRows);

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.ComputeMetricFrom"/>
        public override DoubleMetric ComputeMetricFrom(Option<FrequenciesAndNumRows> state)
        {
            if (!state.HasValue)
            {
                return AnalyzersExt.MetricFromEmpty(this, Name, string.Join(',', Columns),
                    AnalyzersExt.EntityFrom(Columns));
            }

            IEnumerable<Column> aggregations = AggregationFunctions(state.Value.NumRows);
            Row result = state.Value.Frequencies
                .Agg(aggregations.First(),
                    aggregations.Skip(1).ToArray())
                .Collect()
                .FirstOrDefault();

            return FromAggregationResult(result, 0);
        }

        /// <summary>
        /// Converts a value to a <see cref="DoubleMetric"/>.
        /// </summary>
        /// <param name="value">The value to convert into <see cref="DoubleMetric"/>.</param>
        /// <returns>The instance of type <see cref="DoubleMetric"/> that represents the value.</returns>
        protected DoubleMetric ToSuccessMetric(double value) =>
            AnalyzersExt.MetricFromValue(value, Name, string.Join(',', Columns),
                AnalyzersExt.EntityFrom(Columns));

        /// <summary>
        /// Converts an <see cref="Exception"/> to a <see cref="DoubleMetric"/>.
        /// </summary>
        /// <param name="exception">The exception to convert into <see cref="DoubleMetric"/>.</param>
        /// <returns>The instance of type <see cref="DoubleMetric"/>  that represents the exception.</returns>
        public override DoubleMetric ToFailureMetric(Exception exception) =>
            AnalyzersExt.MetricFromFailure(exception, Name, string.Join(',', Columns),
                AnalyzersExt.EntityFrom(Columns));

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        public virtual DoubleMetric FromAggregationResult(Row result, int offset)
        {
            if (result.Values.Length <= offset || result[offset] == null)
            {
                return AnalyzersExt.MetricFromEmpty(this, Name, string.Join(',', Columns),
                    AnalyzersExt.EntityFrom(Columns));
            }

            return ToSuccessMetric(result.GetAs<double>(offset));
        }

        /// <summary>
        /// Overrides the ToString method.
        /// </summary>
        /// <returns>Returns the string identifier of the analyzer in the following format: AnalyzerType(List(column_names), where).</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append("List(")
                .Append(string.Join(",", Columns))
                .Append(")")
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }

    /// <summary>
    /// Base class for all analyzers that operate the frequencies of groups in the data.
    /// </summary>
    public abstract class FrequencyBasedAnalyzer : GroupingAnalyzer<FrequenciesAndNumRows, DoubleMetric>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="FrequencyBasedAnalyzer"/> class.
        /// </summary>
        /// <param name="name">The name of the grouping analyzer.</param>
        /// <param name="columns">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        protected FrequencyBasedAnalyzer(string name, IEnumerable<string> columns, Option<string> where = default)
            : base(name, columns, where)
        {
        }

        /// <inheritdoc cref="GroupingAnalyzer{S,M}.GroupingColumns"/>
        public override IEnumerable<string> GroupingColumns() => Columns;

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.ComputeStateFrom"/>
        public override Option<FrequenciesAndNumRows> ComputeStateFrom(DataFrame dataFrame) =>
            new Option<FrequenciesAndNumRows>(
                ComputeFrequencies(dataFrame, GroupingColumns(),
                    new Option<string>())
            );
        /// <inheritdoc cref="GroupingAnalyzer{S,M}.Preconditions"/>
        public override IEnumerable<Action<StructType>> Preconditions() =>
            new[] { AnalyzersExt.AtLeastOne(Columns) }
                .Concat(Columns.Select(AnalyzersExt.HasColumn))
                .Concat(Columns.Select(AnalyzersExt.IsNotNested))
                .Concat(base.Preconditions());

        /// <summary>
        /// Compute the frequencies of groups in the data, essentially via a query like
        /// SELECT colA, colB, ..., COUNT(*)
        /// FROM DATA
        /// WHERE colA IS NOT NULL OR colB IS NOT NULL OR ...
        /// GROUP BY colA, colB, ..
        /// </summary>
        /// <param name="data">The data to run the computation on <see cref="DataFrame"/>.</param>
        /// <param name="groupingColumns">The grouping columns to use for the group operation.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        /// <returns></returns>
        public static FrequenciesAndNumRows ComputeFrequencies(DataFrame data,
            IEnumerable<string> groupingColumns, Option<string> where)
        {
            IEnumerable<Column> columnsToGroupBy = groupingColumns.Select(name => Col(name));
            IEnumerable<Column> projectionColumns = columnsToGroupBy.Append(Col(AnalyzersExt.COUNT_COL));
            Column atLeasOneNonNullGroupingColumn = groupingColumns.Aggregate(Expr(false.ToString()),
                (condition, name) => condition.Or(Col(name).IsNotNull()));

            //TODO: Add Transoform function
            where = where.GetOrElse("true");

            DataFrame frequencies = data
                .Select(columnsToGroupBy.ToArray())
                .Where(atLeasOneNonNullGroupingColumn)
                .Filter(where.Value)
                .GroupBy(columnsToGroupBy.ToArray())
                .Agg(Count(Lit(1)).Alias(AnalyzersExt.COUNT_COL))
                .Select(projectionColumns.ToArray());

            long numRows = data
                .Select(columnsToGroupBy.ToArray())
                .Where(atLeasOneNonNullGroupingColumn)
                .Filter(where.Value)
                .Count();

            return new FrequenciesAndNumRows(frequencies, numRows);
        }

        /// <summary>
        /// Overrides the ToString method.
        /// </summary>
        /// <returns>Returns the string identifier of the analyzer in the following format: AnalyzerType(List(column_names), where).</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append("List(")
                .Append(string.Join(",", Columns))
                .Append(")")
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }

    /// <summary>
    /// State representing frequencies of groups in the data, as well as overall number of rows
    /// </summary>
    public class FrequenciesAndNumRows : State<FrequenciesAndNumRows>
    {
        /// <summary>
        /// A <see cref="DataFrame"/> representing the frequencies.
        /// </summary>
        public DataFrame Frequencies;
        /// <summary>
        /// The number of rows.
        /// </summary>
        public long NumRows;

        /// <summary>
        /// Initializes a new instance of type <see cref="FrequenciesAndNumRows"/> class.
        /// </summary>
        /// <param name="frequencies">A <see cref="DataFrame"/> representing the frequencies.</param>
        /// <param name="numRows">The number of rows.</param>
        public FrequenciesAndNumRows(DataFrame frequencies, long numRows)
        {
            Frequencies = frequencies;
            NumRows = numRows;
        }

        /// <inheritdoc cref="State{T}.Sum"/>
        public IState Sum(IState other) => base.Sum((FrequenciesAndNumRows)other);

        /// <inheritdoc cref="State{T}.Sum"/>
        public override FrequenciesAndNumRows Sum(FrequenciesAndNumRows other)
        {
            IEnumerable<string> columns = Frequencies.Schema().Fields
                .Select(field => field.Name)
                .Where(field => field != AnalyzersExt.COUNT_COL);

            IEnumerable<Column> projectionAfterMerge = columns
                .Select(col =>
                    Coalesce(Col($"this.{col}"), Col($"other.{col}")).As(col))
                .Append(
                    (AnalyzersExt.ZeroIfNull($"this.{AnalyzersExt.COUNT_COL}") +
                     AnalyzersExt.ZeroIfNull($"other.{AnalyzersExt.COUNT_COL}")).As(AnalyzersExt.COUNT_COL));


            Column joinCondition = columns.Aggregate(NullSafeEq(columns.First()),
                (previous, result) => previous.And(NullSafeEq(result)));


            DataFrame frequenciesSum = Frequencies
                .Alias("this")
                .Join(other.Frequencies.Alias("other"), joinCondition, "outer")
                .Select(projectionAfterMerge.ToArray());


            return new FrequenciesAndNumRows(frequenciesSum, NumRows + other.NumRows);
        }

        private Column NullSafeEq(string column) => Col($"this.{column}") == Col($"other.{column}");
    }
}
