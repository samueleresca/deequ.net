using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class Distinctness : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer, IAnalyzer<DoubleMetric>
    {
        public readonly Option<string> Where;
        public IEnumerable<string> Columns;

        public Distinctness(IEnumerable<string> columns, Option<string> where) : base("Distinctness", columns)
        {
            Columns = columns;
            Where = where;
        }

        public Distinctness(IEnumerable<string> columns) : base("Distinctness", columns)
        {
            Columns = columns;
            Where = Option<string>.None;
        }

        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        public Option<string> FilterCondition() => Where;

        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] { Sum(Col(AnalyzersExt.COUNT_COL).Geq(1).Cast("double")) / numRows };
    }
}
