using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class Histogram : Analyzer<FrequenciesAndNumRows, HistogramMetric>, IFilterableAnalyzer
    {
        public static int MaxDetailBins = 1000;
        public static string NullFieldReplacement = "NullValue";
        public readonly Option<Func<Column, Column>> BinningUdf;
        public readonly string Column;
        private readonly int _maxDetailBins;
        public readonly Option<string> Where;

        public Histogram(string column, Option<string> where, Option<Func<Column, Column>> binningUdf,
            int maxDetailBins)
        {
            BinningUdf = binningUdf;
            Where = where;
            Column = column;
            _maxDetailBins = maxDetailBins;
        }

        public override IEnumerable<Action<StructType>> Preconditions()
        {
            return new[] { PARAM_CHECK(), AnalyzersExt.HasColumn(Column) };
        }

        public override HistogramMetric ToFailureMetric(Exception e)
        {
            return new HistogramMetric(Column, new Try<Distribution>(ExceptionExt.WrapIfNecessary(e)));
        }

        public new HistogramMetric Calculate(DataFrame data)
        {
            return base.Calculate(data);
        }

        public Option<string> FilterCondition()
        {
            return Where;
        }

        public override Option<FrequenciesAndNumRows> ComputeStateFrom(DataFrame dataFrame)
        {
            var totalCount = dataFrame.Count();

            var dataFrameFiltered = FilterOptional(Where, dataFrame);
            var binnedDataFrame = BinOptional(BinningUdf, dataFrameFiltered);

            binnedDataFrame = binnedDataFrame.Select(Col(Column).Cast("string"))
                .Na().Fill(NullFieldReplacement)
                .GroupBy(Column)
                .Count()
                .WithColumnRenamed("count", AnalyzersExt.COUNT_COL);


            return new Option<FrequenciesAndNumRows>(new FrequenciesAndNumRows(binnedDataFrame, totalCount));
        }

        public override HistogramMetric ComputeMetricFrom(Option<FrequenciesAndNumRows> state)
        {
            if (!state.HasValue)
                return new HistogramMetric(Column, new Try<Distribution>(new Exception()));

            var topNRows =
                state.Value.Frequencies.OrderBy(Desc(AnalyzersExt.COUNT_COL)).Take(_maxDetailBins);
            var binCount = state.Value.Frequencies.Count();

            var histogramDetails = topNRows
                .Select(row =>
                {
                    var discreteValue = (string)row[0];
                    var absolute = row.GetAs<int>(1);
                    var ratio = (double)absolute / state.Value.NumRows;

                    return new KeyValuePair<string, DistributionValue>(discreteValue,
                        new DistributionValue(absolute, ratio));
                })
                .ToDictionary(x => x.Key,
                    x => x.Value);

            var distribution = new Distribution(histogramDetails, binCount);

            return new HistogramMetric(Column, distribution);
        }

        private Action<StructType> PARAM_CHECK()
        {
            return _ =>
            {
                if (_maxDetailBins > MaxDetailBins)
                    throw new Exception(
                        $"Cannot return histogram values for more than ${MaxDetailBins} values");
            };
        }

        private DataFrame BinOptional(Option<Func<Column, Column>> binningUdf, DataFrame dataFrame)
        {
            return binningUdf.HasValue switch
            {
                true => dataFrame.WithColumn(Column, binningUdf.Value(Col(Column))),
                false => dataFrame
            };
        }

        private DataFrame FilterOptional(Option<string> where, DataFrame dataFrame)
        {
            return where.HasValue switch
            {
                true => dataFrame.Filter(where.Value),
                false => dataFrame
            };
        }
    }
}