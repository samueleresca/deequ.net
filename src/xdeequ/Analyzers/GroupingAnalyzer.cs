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
    public abstract class ScanShareableFrequencyBasedAnalyzer : FrequencyBasedAnalyzer
    {
        protected ScanShareableFrequencyBasedAnalyzer(string name, IEnumerable<string> columnsToGroupOn) : base(name,
            columnsToGroupOn)
        {
        }

        public abstract IEnumerable<Column> AggregationFunctions(long numRows);

        public override DoubleMetric ComputeMetricFrom(Option<FrequenciesAndNumRows> state)
        {
            if (!state.HasValue)
                return AnalyzersExt.MetricFromEmpty(this, Name, string.Join(',', ColumnsToGroupOn),
                    AnalyzersExt.EntityFrom(ColumnsToGroupOn));

            var aggregations = AggregationFunctions(state.Value.NumRows);
            var result = state.Value.Frequencies
                .Agg(aggregations.First(),
                    aggregations.Skip(1).ToArray())
                .Collect()
                .FirstOrDefault();

            return FromAggregationResult(result, 0);
        }

        protected DoubleMetric ToSuccessMetric(double value)
        {
            return AnalyzersExt.MetricFromValue(value, Name, string.Join(',', ColumnsToGroupOn),
                AnalyzersExt.EntityFrom(ColumnsToGroupOn));
        }

        public override DoubleMetric ToFailureMetric(Exception exception)
        {
            return AnalyzersExt.MetricFromFailure(exception, Name, string.Join(',', ColumnsToGroupOn),
                AnalyzersExt.EntityFrom(ColumnsToGroupOn));
        }

        public DoubleMetric FromAggregationResult(Row result, int offset)
        {
            if (result.Values.Length <= offset || result[offset] == null)
                return AnalyzersExt.MetricFromEmpty(this, Name, string.Join(',', ColumnsToGroupOn),
                    AnalyzersExt.EntityFrom(ColumnsToGroupOn));

            return ToSuccessMetric(result.GetAs<double>(offset));
        }
    }

    public abstract class FrequencyBasedAnalyzer : GroupingAnalyzer<FrequenciesAndNumRows, DoubleMetric>
    {
        public FrequencyBasedAnalyzer(string name, IEnumerable<string> columnsToGroupOn)
        {
            Name = name;
            ColumnsToGroupOn = columnsToGroupOn;
        }

        public string Name { get; set; }
        public IEnumerable<string> ColumnsToGroupOn { get; set; }

        public override IEnumerable<string> GroupingColumns()
        {
            return ColumnsToGroupOn;
        }

        public override Option<FrequenciesAndNumRows> ComputeStateFrom(DataFrame dataFrame)
        {
            return new Option<FrequenciesAndNumRows>(
                ComputeFrequencies(dataFrame, GroupingColumns(),
                    new Option<string>())
            );
        }

        public override IEnumerable<Action<StructType>> Preconditions()
        {
            return new[] {AnalyzersExt.AtLeastOne(ColumnsToGroupOn)}
                .Concat(ColumnsToGroupOn.Select(AnalyzersExt.HasColumn))
                .Concat(ColumnsToGroupOn.Select(AnalyzersExt.IsNotNested))
                .Concat(base.Preconditions());
        }

        public static FrequenciesAndNumRows ComputeFrequencies(DataFrame data,
            IEnumerable<string> groupingColumns, Option<string> where)
        {
            var columnsToGroupBy = groupingColumns.Select(name => Col(name));
            var projectionColumns = columnsToGroupBy.Append(Col(AnalyzersExt.COUNT_COL));
            var atLeasOneNonNullGroupingColumn = groupingColumns.Aggregate(Expr(false.ToString()),
                (condition, name) => condition.Or(Col(name).IsNotNull()));

            //TODO: Add Transoform function
            where = where.GetOrElse("true");

            var frequencies = data
                .Select(columnsToGroupBy.ToArray())
                .Where(atLeasOneNonNullGroupingColumn)
                .Filter(where.Value)
                .GroupBy(columnsToGroupBy.ToArray())
                .Agg(Count(Lit(1)).Alias(AnalyzersExt.COUNT_COL))
                .Select(projectionColumns.ToArray());

            var numRows = data
                .Select(columnsToGroupBy.ToArray())
                .Where(atLeasOneNonNullGroupingColumn)
                .Filter(where.Value)
                .Count();

            return new FrequenciesAndNumRows(frequencies, numRows);
        }
    }


    public class FrequenciesAndNumRows : State<FrequenciesAndNumRows>, IState
    {
        public DataFrame Frequencies;
        public long NumRows;

        public FrequenciesAndNumRows(DataFrame frequencies, long numRows)
        {
            Frequencies = frequencies;
            NumRows = numRows;
        }

        public IState Sum(IState other)
        {
            return base.Sum((FrequenciesAndNumRows) other);
        }

        public override FrequenciesAndNumRows Sum(FrequenciesAndNumRows other)
        {
            var columns = Frequencies.Schema().Fields
                .Select(field => field.Name)
                .Where(field => field != AnalyzersExt.COUNT_COL);

            var projectionAfterMerge = columns
                .Select(col =>
                    Coalesce(Col($"this.{col}"), Col($"other.{col}")).As(col))
                .Append(
                    (AnalyzersExt.ZeroIfNull($"this.{AnalyzersExt.COUNT_COL}") +
                     AnalyzersExt.ZeroIfNull($"other.{AnalyzersExt.COUNT_COL}")).As(AnalyzersExt.COUNT_COL));


            var joinCondition = columns.Aggregate(NullSafeEq(columns.First()),
                (previous, result) => previous.And(NullSafeEq(result)));


            var frequenciesSum = Frequencies
                .Alias("this")
                .Join(other.Frequencies.Alias("other"), joinCondition, "outer")
                .Select(projectionAfterMerge.ToArray());


            return new FrequenciesAndNumRows(frequenciesSum, NumRows + other.NumRows);
        }

        private Column NullSafeEq(string column)
        {
            return Col($"this.{column}") == Col($"other.{column}");
        }
    }
}