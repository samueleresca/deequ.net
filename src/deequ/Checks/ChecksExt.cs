using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Checks
{
    internal static class ChecksExt
    {
        public static Column IsEachNotNull(IEnumerable<string> cols) =>
            cols
                .Select(x => Col(x).IsNotNull())
                .Aggregate((acc, x) => acc.And(x));

        public static Column IsAnyNotNull(IEnumerable<string> cols) =>
            cols
                .Select(x => Col(x).IsNotNull())
                .Aggregate((acc, x) => acc.Or(x));
    }
}
