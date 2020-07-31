using System;
using System.Text.RegularExpressions;

namespace xdeequ.Analyzers.Catalyst
{
    [Serializable]
    public class StatefulDataType
    {
        private const int SIZE_IN_BITES = 5;
        private const int NULL_POS = 0;
        private const int FRATIONAL_POS = 1;
        private const int INTEGRAL_POS = 2;
        private const int BOOLEAN_POS = 3;
        private const int STRING_POS = 4;

        public static Regex FRACTIONAL = new Regex(@"^(-|\+)? ?\d*\.\d*$");
        public static Regex INTEGRAL = new Regex(@"^(-|\+)? ?\d*$");
        public static Regex BOOLEAN = new Regex(@"^(true|false)$");

        public string GetAggregatedColumn() => "arrayDataTypeCount";

        public string[] ColumnNames() =>
            new[] { "nullCount", "fractionalCount", "integralCount", "booleanCount", "stringCount" };

        public int[] Update(string columnValue)
        {
            int[] values = new int[SIZE_IN_BITES];

            if (columnValue == null)
            {
                values[NULL_POS] = values[NULL_POS] + 1;
                return values;
            }

            string value = columnValue;

            if (FRACTIONAL.IsMatch(value))
            {
                values[FRATIONAL_POS] = values[FRATIONAL_POS] + 1;
            }
            else if (INTEGRAL.IsMatch(value))
            {
                values[INTEGRAL_POS] = values[INTEGRAL_POS] + 1;
            }
            else if (BOOLEAN.IsMatch(value))
            {
                values[BOOLEAN_POS] = values[BOOLEAN_POS] + 1;
            }
            else
            {
                values[STRING_POS] = values[STRING_POS] + 1;
            }

            return values;
        }
    }
}
