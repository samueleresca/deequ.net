using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class Entropy : ScanShareableFrequencyBasedAnalyzer
    {
        private readonly Option<string> _where;
        private Option<string> _column;

        public Entropy(Option<string> column, Option<string> where) : base("Entropy",
            new[] { column.Value }.AsEnumerable())
        {
            _column = column;
            _where = where;
        }

        public Entropy(Option<string> column) : base("Entropy", new[] { column.Value }.AsEnumerable())
        {
            _column = column;
            _where = Option<string>.None;
        }

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return base.ToFailureMetric(e);
        }


        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            var summands = Udf<double, double>(count =>
            {
                if (count == 0.0)
                    return 0.0;

                return -(count / numRows) * Math.Log(count / numRows);
            });

            return new[] { Sum(summands(Col(AnalyzersExt.COUNT_COL).Cast("double"))) };
        }
    }
}