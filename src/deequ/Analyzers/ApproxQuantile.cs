using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.Sql;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    internal sealed class ApproxQuantile : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        private readonly string _column;
        private readonly Option<string> _where;

        public ApproxQuantile(string instance, string column, Option<string> where)
            : base("ApproxCountDistinct", instance, Entity.Column)
        {
            _where = where;
            _column = column;
        }

        public Option<string> FilterCondition() => throw new NotImplementedException();

        public override IEnumerable<Column> AggregationFunctions() => new Column[] { };

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            throw new NotImplementedException();

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(_column)
                .Append(",")
                .Append(_where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
