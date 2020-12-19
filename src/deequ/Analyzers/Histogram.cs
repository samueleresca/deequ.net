using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;


namespace deequ.Analyzers
{
    /// <summary>
    /// Histogram is the summary of values in a column of a <see cref="DataFrame"/>. Groups the given column's values,
    /// and calculates the number of rows with that specific value and the fraction of this value.
    /// </summary>
    public sealed class Histogram : Analyzer<FrequenciesAndNumRows, HistogramMetric>, IFilterableAnalyzer
    {
        /// <summary>
        /// Constant that represents the maximum number of bins.
        /// </summary>
        public static int DEFAULT_MAX_DETAIL_BINS = 1000;
        /// <summary>
        ///
        /// </summary>
        public static string NULL_FIELD_REPLACEMENT = "NullValue";
        /// <summary>
        /// Optional binning function to run before grouping to re-categorize the column values.
        /// For example to turn a numerical value to a categorical value binning functions might be used.
        /// </summary>
        public readonly Option<Func<Column, Column>> BinningUdf;
        /// <summary>
        /// Column to do histogram analysis on.
        /// </summary>
        public readonly string Column;
        /// <summary>
        /// Histogram details is only provided for N column values with top counts.
        /// maxBins sets the N. This limit does not affect what is being returned as number of bins. It always returns the
        /// distinct value count.
        /// </summary>
        public readonly int MaxDetailBins;
        /// <summary>
        /// A where clause to filter only some values in a column <see cref="Expr"/>.
        /// </summary>
        public readonly Option<string> Where;

        /// <summary>
        /// Initializes a new instance of type <see cref="Histogram"/> class.
        /// </summary>
        /// <param name="column">Column to do histogram analysis on.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        /// <param name="binningUdf">Optional binning function to run before grouping to re-categorize the column values.</param>
        /// <param name="maxDetailBins">Histogram details is only provided for N column values with top counts.</param>
        public Histogram(string column, Option<string> where, Option<Func<Column, Column>> binningUdf,
            int maxDetailBins)
        {
            BinningUdf = binningUdf;
            Where = where;
            Column = column;
            MaxDetailBins = maxDetailBins;
        }

        /// <summary>
        /// Retrieve the filter condition assigned to the instance.
        /// </summary>
        /// <returns>The filter condition assigned to the instance.</returns>
        public Option<string> FilterCondition() => Where;

        /// <inheritdoc cref="Analyzer{S,M}.Preconditions"/>
        public override IEnumerable<Action<StructType>> Preconditions() =>
            new[] { PARAM_CHECK(), AnalyzersExt.HasColumn(Column) };

        /// <inheritdoc cref="Analyzer{S,M}.ToFailureMetric"/>
        public override HistogramMetric ToFailureMetric(Exception e) =>
            new HistogramMetric(Column, new Try<Distribution>(ExceptionExt.WrapIfNecessary(e)));

        /// <inheritdoc cref="Analyzer{S,M}.Calculate"/>
        public HistogramMetric Calculate(DataFrame data) => base.Calculate(data);

        /// <inheritdoc cref="Analyzer{S,M}.ComputeStateFrom"/>
        public override Option<FrequenciesAndNumRows> ComputeStateFrom(DataFrame dataFrame)
        {
            long totalCount = dataFrame.Count();

            DataFrame dataFrameFiltered = FilterOptional(Where, dataFrame);
            DataFrame binnedDataFrame = BinOptional(BinningUdf, dataFrameFiltered);

            binnedDataFrame = binnedDataFrame.Select(Col(Column).Cast("string"))
                .Na().Fill(NULL_FIELD_REPLACEMENT)
                .GroupBy(Column)
                .Count()
                .WithColumnRenamed("count", AnalyzersExt.COUNT_COL);


            return new Option<FrequenciesAndNumRows>(new FrequenciesAndNumRows(binnedDataFrame, totalCount));
        }

        /// <inheritdoc cref="Analyzer{S,M}.ComputeMetricFrom"/>
        public override HistogramMetric ComputeMetricFrom(Option<FrequenciesAndNumRows> state)
        {
            if (!state.HasValue)
            {
                return new HistogramMetric(Column, new Try<Distribution>(new Exception()));
            }

            IEnumerable<Row> topNRows =
                state.Value.Frequencies.OrderBy(Desc(AnalyzersExt.COUNT_COL)).Take(MaxDetailBins);
            long binCount = state.Value.Frequencies.Count();

            Dictionary<string, DistributionValue> histogramDetails = topNRows
                .Select(row =>
                {
                    string discreteValue = (string)row[0];
                    int absolute = row.GetAs<int>(1);
                    double ratio = (double)absolute / state.Value.NumRows;

                    return new KeyValuePair<string, DistributionValue>(discreteValue,
                        new DistributionValue(absolute, ratio));
                })
                .ToDictionary(keyValuePair => keyValuePair.Key,
                    keyValuePair => keyValuePair.Value);

            Distribution distribution = new Distribution(histogramDetails, binCount);

            return new HistogramMetric(Column, distribution);
        }

        private Action<StructType> PARAM_CHECK() =>
            structType =>
            {
                if (MaxDetailBins > DEFAULT_MAX_DETAIL_BINS)
                {
                    throw new Exception(
                        $"Cannot return histogram values for more than ${DEFAULT_MAX_DETAIL_BINS} values");
                }
            };

        private DataFrame BinOptional(Option<Func<Column, Column>> binningUdf, DataFrame dataFrame) =>
            binningUdf.HasValue switch
        {
            true => dataFrame.WithColumn(Column, binningUdf.Value(Col(Column))),
            false => dataFrame
        };

        private DataFrame FilterOptional(Option<string> where, DataFrame dataFrame) =>
            where.HasValue switch
        {
            true => dataFrame.Filter(where.Value),
            false => dataFrame
        };
    }
}
