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

        public ApproxCountDistinct(string instance, string column, Option<string> where)
            : base("ApproxCountDistinct", instance, MetricEntity.Column, column, where)
        {
            Column = column;
            Where = where;
        }

        public override IEnumerable<Column> AggregationFunctions() => new[] { ApproxCountDistinct(Column.Value) };

        protected override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            throw new NotImplementedException();

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Column)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
