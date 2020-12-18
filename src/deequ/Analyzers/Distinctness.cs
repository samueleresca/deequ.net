using System;
using System.Collections.Generic;
using System.Text;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;


namespace deequ.Analyzers
{
    public sealed class Distinctness : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer
    {
        public Distinctness(IEnumerable<string> columns, Option<string> where) : base("Distinctness", columns, where)
        {
        }

        public Distinctness(IEnumerable<string> columns) : base("Distinctness", columns)
        {
        }

        public Option<string> FilterCondition() => Where;

        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] { Sum(Col(AnalyzersExt.COUNT_COL).Geq(1).Cast("double")) / numRows };

    }
}
