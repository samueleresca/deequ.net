using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class UniqueValueRatio : ScanShareableFrequencyBasedAnalyzer, IFilterableAnalyzer,
        IGroupAnalyzer<FrequenciesAndNumRows, DoubleMetric>
    {
        public readonly Option<string> Where;
        public IEnumerable<string> Columns;

        public UniqueValueRatio(IEnumerable<string> columns, Option<string> where) : base("UniqueValueRatio", columns)
        {
            Columns = columns;
            Where = where;
        }

        public UniqueValueRatio(IEnumerable<string> columns) : base("UniqueValueRatio", columns)
        {
            Columns = columns;
            Where = Option<string>.None;
        }

        public Option<string> FilterCondition() => Where;

        public override DoubleMetric ToFailureMetric(Exception e) => base.ToFailureMetric(e);

        public override IEnumerable<Column> AggregationFunctions(long numRows) =>
            new[] {Sum(Col(AnalyzersExt.COUNT_COL).EqualTo(Lit(1)).Cast("double")), Count("*")};

        public override DoubleMetric FromAggregationResult(Row result, int offset)
        {
            double numUniqueValues = (double)result[offset];
            int numDistinctValues = (int)result[offset + 1];

            return ToSuccessMetric(numUniqueValues / numDistinctValues);
        }
    }
}
