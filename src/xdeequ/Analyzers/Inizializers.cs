using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public static class Initializers
    {
        public static Size Size(Option<string> where)
        {
            return new Size(where);
        }

        public static Histogram Histogram(string column)
        {
            return new Histogram(column, Option<string>.None,
                Option<Func<Column, Column>>.None, 1000);
        }

        public static Histogram Histogram(string column, Option<string> where)
        {
            return new Histogram(column, where, Option<Func<Column, Column>>.None, 1000);
        }

        public static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc)
        {
            return new Histogram(column, Option<string>.None, binningFunc, 1000);
        }

        public static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            int maxDetailBins)
        {
            return new Histogram(column, where, binningFunc, maxDetailBins);
        }

        public static Histogram Histogram(string column, Option<string> where, int maxDetailBins)
        {
            return new Histogram(column, where, Option<Func<Column, Column>>.None, maxDetailBins);
        }

        public static Completeness Completeness(Option<string> column)
        {
            return new Completeness(column);
        }

        public static Completeness Completeness(Option<string> column, Option<string> where)
        {
            return new Completeness(column, where);
        }

        public static Uniqueness Uniqueness(IEnumerable<string> columns)
        {
            return new Uniqueness(columns);
        }

        public static Uniqueness Uniqueness(IEnumerable<string> columns, Option<string> where)
        {
            return new Uniqueness(columns, where);
        }

        public static Uniqueness Uniqueness(Option<string> column, Option<string> where)
        {
            return new Uniqueness(new[] {column.Value}, where);
        }

        public static Distinctness Distinctness(IEnumerable<string> columns, Option<string> where)
        {
            return new Distinctness(columns, where);
        }

        public static UniqueValueRatio UniqueValueRatio(IEnumerable<string> columns, Option<string> where)
        {
            return new UniqueValueRatio(columns, where);
        }

        public static Compliance Compliance(string instance, Column predicate, Option<string> where)
        {
            return new Compliance(instance, predicate, where);
        }

        public static Compliance Compliance(string instance, Column predicate)
        {
            return new Compliance(instance, predicate, Option<string>.None);
        }

        public static MutualInformation MutualInformation(IEnumerable<string> columns)
        {
            return new MutualInformation(columns);
        }

        public static MutualInformation MutualInformation(IEnumerable<string> columns, Option<string> where)
        {
            return new MutualInformation(columns, where);
        }

        public static MutualInformation MutualInformation(Option<string> column, Option<string> where)
        {
            return new MutualInformation(new[] {column.Value}, where);
        }

        public static MaxLength MaxLength(string column)
        {
            return new MaxLength(column, new Option<string>());
        }

        public static MaxLength MaxLength(string column, Option<string> where)
        {
            return new MaxLength(column, where);
        }


        public static MinLength MinLength(string column)
        {
            return new MinLength(column, Option<string>.None);
        }

        public static MinLength MinLength(string column, Option<string> where)
        {
            return new MinLength(column, where);
        }

        public static Minimum Minimum(string column)
        {
            return new Minimum(column, new Option<string>());
        }

        public static Minimum Minimum(string column, Option<string> where)
        {
            return new Minimum(column, where);
        }

        public static Maximum Maximum(string column)
        {
            return new Maximum(column, Option<string>.None);
        }

        public static Maximum Maximum(string column, Option<string> where)
        {
            return new Maximum(column, where);
        }

        public static Mean Mean(string column)
        {
            return new Mean(column, Option<string>.None);
        }

        public static Mean Mean(string column, Option<string> where)
        {
            return new Mean(column, where);
        }

        public static Sum Sum(string column)
        {
            return new Sum(column, Option<string>.None);
        }

        public static Sum Sum(string column, Option<string> where)
        {
            return new Sum(column, where);
        }

        public static StandardDeviation StandardDeviation(string column)
        {
            return new StandardDeviation(column, Option<string>.None);
        }

        public static StandardDeviation StandardDeviation(string column, Option<string> where)
        {
            return new StandardDeviation(column, where);
        }

        public static Entropy Entropy(Option<string> column)
        {
            return new Entropy(column);
        }

        public static Entropy Entropy(Option<string> column, Option<string> where)
        {
            return new Entropy(column, where);
        }

        public static PatternMatch PatternMatch(string column, Regex pattern)
        {
            return new PatternMatch(column, pattern, new Option<string>());
        }

        public static PatternMatch PatternMatch(string column, Regex pattern, Option<string> where)
        {
            return new PatternMatch(column, pattern, where);
        }

        public static DataType DataType(string column)
        {
            return new DataType(column, Option<string>.None);
        }

        public static DataType DataType(string column, Option<string> where)
        {
            return new DataType(column, where);
        }
    }
}