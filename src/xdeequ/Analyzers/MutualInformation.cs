using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql.Types;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class MutualInformation : FrequencyBasedAnalyzer, IFilterableAnalyzer, IAnalyzer<DoubleMetric>
    {
        public readonly Option<string> Where;
        public readonly IEnumerable<string> Columns;

        public MutualInformation(IEnumerable<string> columnsToGroupOn, Option<string> where) :
            base("MutualInformation", columnsToGroupOn)
        {
            Columns = columnsToGroupOn;
            Where = where;
        }

        public MutualInformation(IEnumerable<string> columnsToGroupOn) :
            base("MutualInformation", columnsToGroupOn)
        {
            Columns = columnsToGroupOn;
        }

        public override IEnumerable<Action<StructType>> Preconditions()
        {
            return AnalyzersExt.ExactlyNColumns(Columns, 2).Concat(base.Preconditions());
        }

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return AnalyzersExt.MetricFromFailure(e, "MutualInformation", string.Join(',', Columns),
                Entity.MultiColumn);
        }

        public Option<string> FilterCondition()
        {
            return Where;
        }


        public override DoubleMetric ComputeMetricFrom(Option<FrequenciesAndNumRows> state)
        {
            if (!state.HasValue)
                return AnalyzersExt.MetricFromEmpty(this, "MutualInformation", string.Join(',', Columns),
                    Entity.MultiColumn);

            var total = state.Value.NumRows;
            var col1 = Columns.First();
            var col2 = Columns.Skip(1).First();

            var freqCol1 = $"__deequ_f1_{col1}";
            var freqCol2 = $"__deequ_f2_{col2}";

            var jointStats = state.Value.Frequencies;

            var marginalStats1 = jointStats
                .Select(col1, AnalyzersExt.COUNT_COL)
                .GroupBy(col1)
                .Agg(Sum(AnalyzersExt.COUNT_COL).As(freqCol1));

            var marginalStats2 = jointStats
                .Select(col2, AnalyzersExt.COUNT_COL)
                .GroupBy(col2)
                .Agg(Sum(AnalyzersExt.COUNT_COL).As(freqCol2));


            var miUdf = Udf((double px, double py, double pxy) =>
                pxy / total * Math.Log(pxy / total / (px / total * (py / total))));

            var miCol = $"__deequ_mi_${col1}_$col2";

            var value = jointStats
                .Join(marginalStats1, col1)
                .Join(marginalStats2, col2)
                .WithColumn(miCol,
                    miUdf(Col(freqCol1).Cast("double"), Col(freqCol2).Cast("double"),
                        Col(AnalyzersExt.COUNT_COL).Cast("double")))
                .Agg(Sum(miCol));

            var resultRow = value.First();

            if (resultRow[0] == null)
                return AnalyzersExt.MetricFromEmpty(this, "MutualInformation", string.Join(',', Columns),
                    Entity.MultiColumn);

            return AnalyzersExt.MetricFromValue(resultRow.GetAs<double>(0), "MutualInformation",
                string.Join(',', Columns),
                Entity.MultiColumn);
        }
    }
}