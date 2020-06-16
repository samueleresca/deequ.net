using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class UniqueValueRatio : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer, IAnalyzer<DoubleMetric>
    {
        private readonly Option<string> _where;
        private IEnumerable<string> _columns;

        public UniqueValueRatio(IEnumerable<string> columns, Option<string> where) : base("UniqueValueRatio", columns)
        {
            _columns = columns;
            _where = where;
        }

        public UniqueValueRatio(IEnumerable<string> columns) : base("UniqueValueRatio", columns)
        {
            _columns = columns;
            _where = Option<string>.None;
        }

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return base.ToFailureMetric(e);
        }

        public Option<string> FilterCondition()
        {
            return _where;
        }

        public override IEnumerable<Column> AggregationFunctions(long numRows)
        {
            return new[]
            {
                Sum(Col(AnalyzersExt.COUNT_COL).EqualTo(Lit(1)).Cast("double")), Count("*")
            };
        }

        public new DoubleMetric FromAggregationResult(Row result, int offset)
        {
            var numUniqueValues = (double) result[offset];
            var numDistinctValues = (double) result[offset + 1];

            return ToSuccessMetric(numUniqueValues / numDistinctValues);
        }
    }
}