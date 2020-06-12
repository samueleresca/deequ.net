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
    public class Entropy : ScanShareableFrequencyBasedAnalyzer, IAnalyzer<DoubleMetric>
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

        public static Entropy Create(Option<string> column) => new Entropy(column);

        public static Entropy Create(Option<string> column, Option<string> where) =>
            new Entropy(column, where);

        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            Func<Column, Column> summands = Udf<double, double>((double count) =>
            {
                if (count == 0.0)
                    return 0.0;

                return -(count / numRows) * Math.Log(count / numRows);
            });

            return new[] { Sum(summands(Col(AnalyzersExt.COUNT_COL).Cast("double"))) };
        }

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return base.ToFailureMetric(e);
        }
    }
}