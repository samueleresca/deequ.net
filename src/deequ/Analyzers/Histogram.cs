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
    internal sealed class Histogram : Analyzer<FrequenciesAndNumRows, HistogramMetric>, IFilterableAnalyzer
    {
        public static int DEFAULT_MAX_DETAIL_BINS = 1000;
        public static string NULL_FIELD_REPLACEMENT = "NullValue";
        public readonly Option<Func<Column, Column>> BinningUdf;
        public readonly string Column;
        public readonly int MaxDetailBins;
        public readonly Option<string> Where;

        public Histogram(string column, Option<string> where, Option<Func<Column, Column>> binningUdf,
            int maxDetailBins)
        {
            BinningUdf = binningUdf;
            Where = where;
            Column = column;
            MaxDetailBins = maxDetailBins;
        }

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Action<StructType>> Preconditions() =>
            new[] { PARAM_CHECK(), AnalyzersExt.HasColumn(Column) };

        public override HistogramMetric ToFailureMetric(Exception e) =>
            new HistogramMetric(Column, new Try<Distribution>(ExceptionExt.WrapIfNecessary(e)));

        public HistogramMetric Calculate(DataFrame data) => base.Calculate(data);

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
