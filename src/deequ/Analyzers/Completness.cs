using System;
using System.Collections.Generic;
using System.Text;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    internal sealed class Completeness : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        public readonly Option<string> Column;
        public readonly Option<string> Where;

        public Completeness(Option<string> column, Option<string> where) : base("Completeness", column.Value,
            Entity.Column)
        {
            Column = column;
            Where = where;
        }

        public Completeness(Option<string> column) : base("Completeness", column.Value, Entity.Column)
        {
            Column = column;
            Where = Option<string>.None;
        }

        public Option<string> FilterCondition() => Where;

        public override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => new NumMatchesAndCount(
                    result.GetAs<int>(offset),
                    result.GetAs<int>(offset + 1)), 2);

        public override IEnumerable<Column> AggregationFunctions()
        {
            Column summarization = Sum(AnalyzersExt.ConditionalSelection(Column, Where)
                .IsNotNull()
                .Cast("int"));

            Column conditional = AnalyzersExt.ConditionalCount(Where);

            return new[] { summarization, conditional };
        }

        public override IEnumerable<Action<StructType>> AdditionalPreconditions() =>
            new[] { AnalyzersExt.HasColumn(Column.Value), AnalyzersExt.IsNotNested(Column.Value) };

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Column)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
