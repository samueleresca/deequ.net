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
    /// Mutual Information describes how much information about one column can be inferred from another
    /// column. If two columns are independent of each other, then nothing can be inferred from one column about
    /// the other, and mutual information is zero. If there is a functional dependency of one column to
    /// another and vice versa, then all information of the two columns are shared, and mutual information is the entropy of each column.
    /// </summary>
    public sealed class MutualInformation : FrequencyBasedAnalyzer
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="MutualInformation"/> class.
        /// </summary>
        /// <param name="columns">The target column names.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MutualInformation(IEnumerable<string> columns, Option<string> where) :
            base("MutualInformation", columns, where)
        {
        }

        /// <summary>
        ///  Initializes a new instance of type <see cref="MutualInformation"/> class.
        /// </summary>
        /// <param name="columns">The target column names.</param>
        public MutualInformation(IEnumerable<string> columns) :
            base("MutualInformation", columns)
        {
        }

        /// <inheritdoc cref="FrequencyBasedAnalyzer.Preconditions"/>
        public override IEnumerable<Action<StructType>> Preconditions() =>
            AnalyzersExt.ExactlyNColumns(Columns, 2).Concat(base.Preconditions());

        /// <inheritdoc cref="FrequencyBasedAnalyzer.ToFailureMetric"/>
        public override DoubleMetric ToFailureMetric(Exception e) =>
            AnalyzersExt.MetricFromFailure(e, "MutualInformation", string.Join(',', Columns),
                MetricEntity.Multicolumn);

        /// <inheritdoc cref="FrequencyBasedAnalyzer.ComputeMetricFrom"/>
        public override DoubleMetric ComputeMetricFrom(Option<FrequenciesAndNumRows> state)
        {
            if (!state.HasValue)
            {
                return AnalyzersExt.MetricFromEmpty(this, "MutualInformation", string.Join(',', Columns),
                    MetricEntity.Multicolumn);
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
                    MetricEntity.Multicolumn);
            }

            return AnalyzersExt.MetricFromValue(resultRow.GetAs<double>(0), "MutualInformation",
                string.Join(',', Columns),
                MetricEntity.Multicolumn);
        }
    }
}
