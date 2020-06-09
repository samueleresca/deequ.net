using Apache.Arrow;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers.Catalyst;
using Column = Microsoft.Spark.Sql.Column;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Extensions
{
    public static class StatefulExt
    {
        public static Column StatefulDataType(Column column)
        {
            var statefulDataType = new StatefulDataType();


            return Column("");
        }
    }
}