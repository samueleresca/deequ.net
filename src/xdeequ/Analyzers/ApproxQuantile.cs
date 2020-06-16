using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public class ApproxQuantile : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        private readonly string _column;
        private readonly Option<string> _where;

        public ApproxQuantile(string instance, string column, Option<string> where)
            : base("ApproxCountDistinct", instance, Entity.Column)
        {
            _where = where;
            _column = column;
        }

        public Option<string> FilterCondition()
        {
            throw new NotImplementedException();
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            throw new NotImplementedException();
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
        {
            throw new NotImplementedException();
        }
    }
}