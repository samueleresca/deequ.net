using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public sealed class MutualInformation : FrequencyBasedAnalyzer, IFilterableAnalyzer
    {
        public readonly IEnumerable<string> Columns;
        public readonly Option<string> Where;

        public MutualInformation(IEnumerable<string> columnsToGroupOn, Option<string> where) :
            base("MutualInformation", columnsToGroupOn)
        {
            Columns = columnsToGroupOn;
            Where = where;
        }

        public MutualInformation(IEnumerable<string> columnsToGroupOn) :
            base("MutualInformation", columnsToGroupOn) =>
            Columns = columnsToGroupOn;

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Action<StructType>> Preconditions() =>
            AnalyzersExt.ExactlyNColumns(Columns, 2).Concat(base.Preconditions());

        public override DoubleMetric ToFailureMetric(Exception e) =>
            AnalyzersExt.MetricFromFailure(e, "MutualInformation", string.Join(',', Columns),
                Entity.Multicolumn);


        public override DoubleMetric ComputeMetricFrom(Option<FrequenciesAndNumRows> state)
        {
            if (!state.HasValue)
            {
                return AnalyzersExt.MetricFromEmpty(this, "MutualInformation", string.Join(',', Columns),
                    Entity.Multicolumn);
            }

            long total = state.Value.NumRows;
            string col1 = Columns.First();
            string col2 = Columns.Skip(1).First();

            string freqCol1 = $"__deequ_f1_{col1}";
            string freqCol2 = $"__deequ_f2_{col2}";

            DataFrame jointStats = state.Value.Frequencies;

            DataFrame marginalStats1 = jointStats
                .Select(col1, AnalyzersExt.COUNT_COL)
                .GroupBy(col1)
                .Agg(Sum(AnalyzersExt.COUNT_COL).As(freqCol1));

            DataFrame marginalStats2 = jointStats
                .Select(col2, AnalyzersExt.COUNT_COL)
                .GroupBy(col2)
                .Agg(Sum(AnalyzersExt.COUNT_COL).As(freqCol2));


            Func<Column, Column, Column, Column> miUdf = Udf((double px, double py, double pxy) =>
                pxy / total * Math.Log(pxy / total / (px / total * (py / total))));

            string miCol = $"__deequ_mi_${col1}_$col2";

            DataFrame value = jointStats
                .Join(marginalStats1, col1)
                .Join(marginalStats2, col2)
                .WithColumn(miCol,
                    miUdf(Col(freqCol1).Cast("double"), Col(freqCol2).Cast("double"),
                        Col(AnalyzersExt.COUNT_COL).Cast("double")))
                .Agg(Sum(miCol));

            Row resultRow = value.First();

            if (resultRow[0] == null)
            {
                return AnalyzersExt.MetricFromEmpty(this, "MutualInformation", string.Join(',', Columns),
                    Entity.Multicolumn);
            }

            return AnalyzersExt.MetricFromValue(resultRow.GetAs<double>(0), "MutualInformation",
                string.Join(',', Columns),
                Entity.Multicolumn);
        }

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
}
