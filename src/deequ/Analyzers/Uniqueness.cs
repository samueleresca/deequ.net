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
    public sealed class Uniqueness : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer
    {

        public Uniqueness(IEnumerable<string> columns, Option<string> where) : base("Uniqueness", columns, where)
        {
        }

        public Uniqueness(IEnumerable<string> columns) : base("Uniqueness", columns)
        {
        }

        public Option<string> FilterCondition() => Where;

        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] { Sum(Col(AnalyzersExt.COUNT_COL).EqualTo(Lit(1)).Cast("double")) / numRows };
    }
}
