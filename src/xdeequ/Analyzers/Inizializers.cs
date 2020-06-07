using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public static class Inizializers
    {
        public static Size Size(Option<string> where) => new Size(where);

        public static Histogram Histogram(string column) => new Histogram(column, new Option<string>(),
            new Option<Func<Column, Column>>(), 1000);

        public static Histogram Histogram(string column, Option<string> where) =>
            new Histogram(column, where, new Option<Func<Column, Column>>(), 1000);

        public static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc) =>
            new Histogram(column, new Option<string>(), binningFunc, 1000);

        public static Histogram Histogram(string column, Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            int maxDetailBins) =>
            new Histogram(column, where, binningFunc, maxDetailBins);

        public static Histogram Histogram(string column, Option<string> where, int maxDetailBins) =>
            new Histogram(column, where, new Option<Func<Column, Column>>(), maxDetailBins);

        public static Completeness Completeness(Option<string> column) => new Completeness(column);

        public static Completeness Completeness(Option<string> column, Option<string> where) =>
            new Completeness(column, where);

        public static Uniqueness Uniqueness(IEnumerable<string> columns) => new Uniqueness(columns);

        public static Uniqueness Uniqueness(Option<string> column, Option<string> where) =>
            new Uniqueness(new[] {column.Value}, where);

        public static Distinctness Distinctness(IEnumerable<string> columns, Option<string> where) =>
            new Distinctness(columns, where);

        public static UniqueValueRatio UniqueValueRatio(IEnumerable<string> columns, Option<string> where) =>
            new UniqueValueRatio(columns, where);

        public static Compliance Compliance(string instance, string predicate, Option<string> where) =>
            new Compliance(instance, predicate, where);

        public static Compliance Compliance(string instance, string predicate) =>
            new Compliance(instance, predicate, new Option<string>());
    }
}