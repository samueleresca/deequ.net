using System;
using System.Collections.Generic;
using System.Text;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    internal sealed class ApproxCountDistinct : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
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
