using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    internal sealed class Distinctness : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer
    {
        public readonly IEnumerable<string> Columns;
        public readonly Option<string> Where;

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

        public Option<string> FilterCondition() => Where;

        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] {Sum(Col(AnalyzersExt.COUNT_COL).Geq(1).Cast("double")) / numRows};

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
