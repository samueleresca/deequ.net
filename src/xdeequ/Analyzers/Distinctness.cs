using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class Distinctness : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer
    {
        private readonly Option<string> _where;
        private IEnumerable<string> _columns;

        public Distinctness(IEnumerable<string> columns, Option<string> where) : base("Distinctness", columns)
        {
            _columns = columns;
            _where = where;
        }

        public Distinctness(IEnumerable<string> columns) : base("Distinctness", columns)
        {
            _columns = columns;
            _where = Option<string>.None;
        }

        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            return new[]
            {
                (Sum(Col(AnalyzersExt.COUNT_COL).Geq(1).Cast("double")) / numRows)
            };
        }

        public Option<string> FilterCondition() => _where;

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return base.ToFailureMetric(e);
        }
    }
}