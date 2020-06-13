using System;
using System.Collections.Generic;
using System.Linq;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Checks
{
    public static class ChecksExt
    {
        public static string IsEachNotNull(IEnumerable<string> cols)
        {
            cols
                .Select(x => Col(x).IsNotNull());
            //.All(x => x.Cast("bool"));
            return String.Empty;
        }

        public static string IsAnyNotNull(IEnumerable<string> cols)
        {
            cols
                .Select(x => Col(x).IsNotNull());
            //.All(x => x.Cast("bool"));
            return String.Empty;
        }
    }
}