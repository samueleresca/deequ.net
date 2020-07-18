using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public sealed class PatternMatch : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        public readonly string Column;
        public readonly Regex Regex;
        public readonly Option<string> Where;

        public PatternMatch(string column, Regex regex, Option<string> where)
            : base("PatternMatch", column, Entity.Column)
        {
            Regex = regex;
            Column = column;
            Where = where;
        }

        public Option<string> FilterCondition() => Where;


        public override IEnumerable<Column> AggregationFunctions()
        {
            Column expression =
                When(RegexpExtract(Column(Column), Regex.ToString(), 0) != Lit(""), 1)
                    .Otherwise(0);

            Column summation = Sum(AnalyzersExt.ConditionalSelection(expression, Where).Cast("integer"));

            return new[] {summation, AnalyzersExt.ConditionalCount(Where)};
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatchesAndCount(
                    (int)result.Get(offset), (int)result.Get(offset + 1)), 2);

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] {AnalyzersExt.HasColumn(Column), AnalyzersExt.IsString(Column)};


        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(string.Join(",", Column))
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }


    public static class Patterns
    {
        // scalastyle:off
        // http://emailregex.com
        public static Regex Email => new Regex(
            @"(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\""(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\\"")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])");

        // https://mathiasbynens.be/demo/url-regex stephenhay
        public static Regex Url => new Regex(@"(https?|ftp)://[^\s/$.?#].[^\s]*");

        public static Regex SocialSecurityNumberUs => new Regex(
            @"((?!219-09-9999|078-05-1120)(?!666|000|9\d{2})\d{3}-(?!00)\d{2}-(?!0{4})\d{4})|((?!219 09 9999|078 05 1120)(?!666|000|9\d{2})\d{3} (?!00)\d{2} (?!0{4})\d{4})|((?!219099999|078051120)(?!666|000|9\d{2})\d{3}(?!00)\d{2}(?!0{4})\d{4})");

        public static Regex CreditCard =>
            new Regex(
                @"\b(?:3[47]\d{2}([\ \-]?)\d{6}\1\d|(?:(?:4\d|5[1-5]|65)\d{2}|6011)([\ \-]?)\d{4}\2\d{4}\2)\d{4}\b");
    }
}
