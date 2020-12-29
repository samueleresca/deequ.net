using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers
{
    public static class Initializers
    {
        public static Size Size(Option<string> where = default) => new Size(where);

        public static Histogram Histogram(string column) =>
            new Histogram(column, Option<string>.None,
                Option<Func<Column, Column>>.None, 1000);

        public static Histogram Histogram(string column, Option<string> where) =>
            new Histogram(column, where, Option<Func<Column, Column>>.None, 1000);

        public static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc) =>
            new Histogram(column, Option<string>.None, binningFunc, 1000);

        public static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            int maxDetailBins) =>
            new Histogram(column, where, binningFunc, maxDetailBins);

        public static Histogram Histogram(string column, Option<string> where, int maxDetailBins) =>
            new Histogram(column, where, Option<Func<Column, Column>>.None, maxDetailBins);

        public static Completeness Completeness(Option<string> column) => new Completeness(column);

        public static Completeness Completeness(Option<string> column, Option<string> where) =>
            new Completeness(column, where);

        public static Uniqueness Uniqueness(IEnumerable<string> columns) => new Uniqueness(columns);

        public static Uniqueness Uniqueness(IEnumerable<string> columns, Option<string> where) =>
            new Uniqueness(columns, where);

        public static Uniqueness Uniqueness(Option<string> column, Option<string> where) =>
            new Uniqueness(new[] { column.Value }, where);

        public static Distinctness Distinctness(IEnumerable<string> columns, Option<string> where) =>
            new Distinctness(columns, where);

        public static UniqueValueRatio UniqueValueRatio(IEnumerable<string> columns, Option<string> where) =>
            new UniqueValueRatio(columns, where);

        public static Compliance Compliance(string instance, Column predicate, Option<string> where) =>
            new Compliance(instance, predicate, where);

        public static Compliance Compliance(string instance, Column predicate) =>
            new Compliance(instance, predicate, Option<string>.None);

        public static MutualInformation MutualInformation(IEnumerable<string> columns) =>
            new MutualInformation(columns);

        public static MutualInformation MutualInformation(IEnumerable<string> columns, Option<string> where) =>
            new MutualInformation(columns, where);

        public static MutualInformation MutualInformation(Option<string> column, Option<string> where) =>
            new MutualInformation(new[] { column.Value }, where);

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

        public static Correlation Correlation(string columnA, string columnB) =>
            new Correlation(columnA, columnB);

        public static Correlation Correlation(string columnA, string columnB, Option<string> where) =>
            new Correlation(columnA, columnB, where);

        public static Entropy Entropy(Option<string> column) => new Entropy(column);

        public static Entropy Entropy(Option<string> column, Option<string> where) => new Entropy(column, where);

        public static PatternMatch PatternMatch(string column, Regex pattern) =>
            new PatternMatch(column, pattern, new Option<string>());

        public static PatternMatch PatternMatch(string column, Regex pattern, Option<string> where) =>
            new PatternMatch(column, pattern, where);

        public static DataType DataType(string column) => new DataType(column, Option<string>.None);

        public static DataType DataType(string column, Option<string> where) => new DataType(column, where);
    }
}
