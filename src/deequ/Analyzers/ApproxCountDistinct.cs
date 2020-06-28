using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public class ApproxCountDistinct : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        private readonly string _column;
        private readonly Option<string> _where;

        public ApproxCountDistinct(string instance, string column, Option<string> where)
            : base("ApproxCountDistinct", instance, Entity.Column)
        {
            _where = where;
            _column = column;
        }

        public Option<string> FilterCondition() => _where;

        public override IEnumerable<Column> AggregationFunctions() => new[] { ApproxCountDistinct(_column) };

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            throw new NotImplementedException();
    }
}
