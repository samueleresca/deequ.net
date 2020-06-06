using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.Analyzers
{
    public class PatternMatch : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        private readonly Option<string> _where;
        private readonly string _column;
        private readonly Regex _regex;

        public PatternMatch(string column, Regex regex, Option<string> where)
            : base("PatternMatch", column, Entity.Column)
        {
            _regex = regex;
            _column = column;
            _where = where;
        }
        
        public static PatternMatch Create(string column, string pattern) =>
            new PatternMatch(column, new Regex(pattern), new Option<string>());

        public static PatternMatch Create(string column, string pattern, Option<string> where) =>
            new PatternMatch(column, new Regex(pattern), where);

        public override IEnumerable<Column> AggregationFunctions()
        {
            var expression =
                When(RegexpExtract(Column(_column), _regex.ToString(), 0) != Lit(""), 1)
                    .Otherwise(0);

            var summation = Sum(AnalyzersExt.ConditionalSelection(expression, _where).Cast("integer"));

            return new[] {summation, AnalyzersExt.ConditionalCount(_where)};
        }

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset)
        {
            return AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatchesAndCount(
                    (int) result.Get(offset), (int) result.Get(offset + 1)), howMany: 2);
        }

        public Option<string> FilterCondition() => _where;

        public override IEnumerable<Action<StructType>> AdditionalPreconditions()
        {
            return new[] {AnalyzersExt.HasColumn(_column), AnalyzersExt.IsString(_column)};
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