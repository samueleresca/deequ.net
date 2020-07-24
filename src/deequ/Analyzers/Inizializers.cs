using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public static class Initializers
    {
        public static Size Size(Option<string> where) => new Size(where);

        internal static Histogram Histogram(string column) =>
            new Histogram(column, Option<string>.None,
                Option<Func<Column, Column>>.None, 1000);

        internal static Histogram Histogram(string column, Option<string> where) =>
            new Histogram(column, where, Option<Func<Column, Column>>.None, 1000);

        internal static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc) =>
            new Histogram(column, Option<string>.None, binningFunc, 1000);

        internal static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            int maxDetailBins) =>
            new Histogram(column, where, binningFunc, maxDetailBins);

        internal static Histogram Histogram(string column, Option<string> where, int maxDetailBins) =>
            new Histogram(column, where, Option<Func<Column, Column>>.None, maxDetailBins);

        internal static Completeness Completeness(Option<string> column) => new Completeness(column);

        internal static Completeness Completeness(Option<string> column, Option<string> where) =>
            new Completeness(column, where);

        internal static Uniqueness Uniqueness(IEnumerable<string> columns) => new Uniqueness(columns);

        internal static Uniqueness Uniqueness(IEnumerable<string> columns, Option<string> where) =>
            new Uniqueness(columns, where);

        internal static Uniqueness Uniqueness(Option<string> column, Option<string> where) =>
            new Uniqueness(new[] {column.Value}, where);

        internal static Distinctness Distinctness(IEnumerable<string> columns, Option<string> where) =>
            new Distinctness(columns, where);

        internal static UniqueValueRatio UniqueValueRatio(IEnumerable<string> columns, Option<string> where) =>
            new UniqueValueRatio(columns, where);

        internal static Compliance Compliance(string instance, Column predicate, Option<string> where) =>
            new Compliance(instance, predicate, where);

        internal static Compliance Compliance(string instance, Column predicate) =>
            new Compliance(instance, predicate, Option<string>.None);

        internal static MutualInformation MutualInformation(IEnumerable<string> columns) =>
            new MutualInformation(columns);

        internal static MutualInformation MutualInformation(IEnumerable<string> columns, Option<string> where) =>
            new MutualInformation(columns, where);

        internal static MutualInformation MutualInformation(Option<string> column, Option<string> where) =>
            new MutualInformation(new[] {column.Value}, where);

        public static MaxLength MaxLength(string column) => new MaxLength(column, new Option<string>());

        public static MaxLength MaxLength(string column, Option<string> where) => new MaxLength(column, where);


        public static MinLength MinLength(string column) => new MinLength(column, Option<string>.None);

        public static MinLength MinLength(string column, Option<string> where) => new MinLength(column, where);

        public static Minimum Minimum(string column) => new Minimum(column, new Option<string>());

        public static Minimum Minimum(string column, Option<string> where) => new Minimum(column, where);

        public static Maximum Maximum(string column) => new Maximum(column, Option<string>.None);

        public static Maximum Maximum(string column, Option<string> where) => new Maximum(column, where);

        public static Mean Mean(string column) => new Mean(column, Option<string>.None);

        public static Mean Mean(string column, Option<string> where) => new Mean(column, where);

        public static Sum Sum(string column) => new Sum(column, Option<string>.None);

        public static Sum Sum(string column, Option<string> where) => new Sum(column, where);

        public static StandardDeviation StandardDeviation(string column) =>
            new StandardDeviation(column, Option<string>.None);

        public static StandardDeviation StandardDeviation(string column, Option<string> where) =>
            new StandardDeviation(column, where);

        internal static Entropy Entropy(Option<string> column) => new Entropy(column);

        internal static Entropy Entropy(Option<string> column, Option<string> where) => new Entropy(column, where);

        public static PatternMatch PatternMatch(string column, Regex pattern) =>
            new PatternMatch(column, pattern, new Option<string>());

        public static PatternMatch PatternMatch(string column, Regex pattern, Option<string> where) =>
            new PatternMatch(column, pattern, where);

        internal static DataType DataType(string column) => new DataType(column, Option<string>.None);

        internal static DataType DataType(string column, Option<string> where) => new DataType(column, where);
    }
}
