using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;

namespace deequ.Analyzers
{
    public static class Initializers
    {
        public static Size Size(Option<string> where = default) => new Size(where);

       public static Histogram Histogram(string column) =>
            new Histogram(column, Option<UserDefinedFunction>.None,1000);

        public static Histogram Histogram(string column, Option<string> where) =>
            new Histogram(column,  Option<UserDefinedFunction>.None, 1000, where);

        public static Histogram Histogram(string column, Option<UserDefinedFunction> binningFunc) =>
            new Histogram(column, binningFunc, 1000);

        public static Histogram Histogram(string column,
            Option<UserDefinedFunction> binningFunc,
            int maxDetailBins,
            Option<string> where
            ) =>
            new Histogram(column,binningFunc, maxDetailBins,  where);
        public static Completeness Completeness(string column) => new Completeness(column);

        public static Completeness Completeness(string column, Option<string> where) =>
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

        public static Compliance Compliance(string instance, Option<string> predicate, Option<string> where) =>
            new Compliance(instance, predicate, where);

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

        public static Entropy Entropy(string column) => new Entropy(column, Option<string>.None);

        public static Entropy Entropy(string column, Option<string> where) => new Entropy(column, where);

        public static PatternMatch PatternMatch(string column, Regex pattern) =>
            new PatternMatch(column, pattern.ToString(), new Option<string>());

        public static PatternMatch PatternMatch(string column, Regex pattern, Option<string> where) =>
            new PatternMatch(column, pattern.ToString(), where);

        public static DataType DataType(string column) => new DataType(column, Option<string>.None);

        public static DataType DataType(string column, Option<string> where) => new DataType(column, where);
    }
}
