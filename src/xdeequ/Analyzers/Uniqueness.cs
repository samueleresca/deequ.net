using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class Uniqueness : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer,
        IGroupAnalyzer<FrequenciesAndNumRows, DoubleMetric>
    {
        public readonly Option<string> Where;
        public IEnumerable<string> Columns;

        public Uniqueness(IEnumerable<string> columns, Option<string> where) : base("Uniqueness", columns)
        {
            Columns = columns;
            Where = where;
        }

        public Uniqueness(IEnumerable<string> columns) : base("Uniqueness", columns)
        {
            Columns = columns;
            Where = Option<string>.None;
        }

        public Option<string> FilterCondition()
        {
            return Where;
        }

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return base.ToFailureMetric(e);
        }

        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            return new[]
            {
                Sum(Col(AnalyzersExt.COUNT_COL).EqualTo(Lit(1)).Cast("double")) / numRows
            };
        }
    }
}