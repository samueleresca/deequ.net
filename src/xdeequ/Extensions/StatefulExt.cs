using Microsoft.Spark.Sql;
using xdeequ.Analyzers.Catalyst;
using Column = Microsoft.Spark.Sql.Column;


namespace xdeequ.Extensions
{
    public static class StatefulExt
    {
        public static Column StatefulDataType(Column column)
        {
            var statefulDataType = new StatefulDataType();

            statefulDataType.Update(new GenericRow(new[] {column}));
            return column;
        }
    }
}