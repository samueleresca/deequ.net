using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public class ApproxQuantile : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        private readonly Option<string> _where;
        private readonly string _column;

        public ApproxQuantile(string instance, string column, Option<string> where)
            : base("ApproxCountDistinct", instance, Entity.Column)
        {
            _where = where;
            _column = column;
        }

        public override Option<NumMatchesAndCount> ComputeStateFrom(DataFrame dataFrame)
        {
            return base.ComputeStateFrom(dataFrame);
        }

        public override IEnumerable<Column> AggregationFunctions()
        {
            throw new System.NotImplementedException();
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
        {
            throw new System.NotImplementedException();
        }

        public Option<string> FilterCondition()
        {
            throw new System.NotImplementedException();
        }
    }
}