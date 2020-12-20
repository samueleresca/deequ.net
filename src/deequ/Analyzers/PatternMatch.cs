using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;


namespace deequ.Analyzers
{

    /// <summary>
    /// PatternMatch is a measure of the fraction of rows that complies with a given column regex constraint.
    /// E.g if the constraint is Patterns.CREDITCARD and the data frame has 5 rows which contain a credit card number in a certain column
    /// according to the regex and and 10 rows that do not, a DoubleMetric would be
    /// returned with 0.33 as value
    /// </summary>
    public sealed class PatternMatch : StandardScanShareableAnalyzer<NumMatchesAndCount>
    {
        /// <summary>
        /// Column to do the pattern match analysis on.
        /// </summary>
        public readonly Regex Regex;

        /// <summary>
        /// Initializes a new instance of type <see cref="PatternMatch"/> class.
        /// </summary>
        /// <param name="column">Column to do the pattern match analysis on.</param>
        /// <param name="regex">The regular expression to check for.</param>
        /// <param name="where">Additional filter to apply before the analyzer is run.</param>
        public PatternMatch(string column, Regex regex, Option<string> where)
            : base("PatternMatch", column, MetricEntity.Column, column, where)
        {
            Regex = regex;
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions()
        {
            Column expression =
                When(RegexpExtract(Column(Column.GetOrElse(string.Empty)), Regex.ToString(), 0) != Lit(""), 1)
                    .Otherwise(0);

            Column summation = Sum(AnalyzersExt.ConditionalSelection(expression, Where).Cast("integer"));

            return new[] { summation, AnalyzersExt.ConditionalCount(Where) };
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>
        protected override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatchesAndCount(
                    (int)result.Get(offset), (int)result.Get(offset + 1)), 2);

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AdditionalPreconditions"/>
        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsString(Column.GetOrElse(string.Empty)) };
    }

    /// <summary>
    /// Provides some default patterns used by the <see cref="PatternMatch"/> analyzer.
    /// </summary>
    public static class Patterns
    {
        /// <summary>
        /// Email pattern.
        /// </summary>
        public static Regex Email => new Regex(
            @"(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\""(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\\"")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])");

        /// <summary>
        /// Url pattern.
        /// </summary>
        public static Regex Url => new Regex(@"(https?|ftp)://[^\s/$.?#].[^\s]*");

        /// <summary>
        /// SSN number.
        /// </summary>
        public static Regex SocialSecurityNumberUs => new Regex(
            @"((?!219-09-9999|078-05-1120)(?!666|000|9\d{2})\d{3}-(?!00)\d{2}-(?!0{4})\d{4})|((?!219 09 9999|078 05 1120)(?!666|000|9\d{2})\d{3} (?!00)\d{2} (?!0{4})\d{4})|((?!219099999|078051120)(?!666|000|9\d{2})\d{3}(?!00)\d{2}(?!0{4})\d{4})");

        /// <summary>
        /// Credit card pattern.
        /// </summary>
        public static Regex CreditCard =>
            new Regex(
                @"\b(?:3[47]\d{2}([\ \-]?)\d{6}\1\d|(?:(?:4\d|5[1-5]|65)\d{2}|6011)([\ \-]?)\d{4}\2\d{4}\2)\d{4}\b");
    }
}
