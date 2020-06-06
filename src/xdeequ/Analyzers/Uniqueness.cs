using System;
using System.Collections.Generic;
using static Microsoft.Spark.Sql.Functions;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public class Uniqueness : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer
    {
        private readonly Option<string> _where;
        private IEnumerable<string> _columns;

        public Uniqueness(IEnumerable<string> columns, Option<string> where) : base("Uniqueness", columns)
        {
            _columns = columns;
            _where = where;
        }

        public Uniqueness(IEnumerable<string> columns) : base("Uniqueness", columns)
        {
            _columns = columns;
            _where = Option<string>.None;
        }

        public static Uniqueness Create(IEnumerable<string> columns) => new Uniqueness(columns);

        public static Uniqueness Create(Option<string> column, Option<string> where) =>
            new Uniqueness(new[] {column.Value}, where);

        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            return new[]
            {
                Sum(Col(AnalyzersExt.COUNT_COL).EqualTo(Lit(1)).Cast("double")) / numRows
            };
        }

        public Option<string> FilterCondition() => _where;

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return base.ToFailureMetric(e);
        }
    }
}