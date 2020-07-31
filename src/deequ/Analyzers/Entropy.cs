using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    internal sealed class Entropy : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer
    {
        public readonly Option<string> Column;
        public readonly Option<string> Where;

        public Entropy(Option<string> column, Option<string> where) : base("Entropy",
            new[] { column.Value }.AsEnumerable())
        {
            Column = column;
            Where = where;
        }

        public Entropy(Option<string> column) : base("Entropy", new[] { column.Value }.AsEnumerable())
        {
            Column = column;
            Where = Option<string>.None;
        }

        public Option<string> FilterCondition() => Where;

        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);


        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            Func<Column, Column> summands = Udf<double, double>(count =>
            {
                if (count == 0.0)
                {
                    return 0.0;
                }

                return -(count / numRows) * Math.Log(count / numRows);
            });

            return new[] { Sum(summands(Col(AnalyzersExt.COUNT_COL).Cast("double"))) };
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Column)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
