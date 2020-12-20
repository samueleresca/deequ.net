using System.Collections.Generic;
using System.Text;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;


namespace deequ.Analyzers
{
    /// <summary>
    /// Compliance is a measure of the fraction of rows that complies with the given column constraint.
    /// E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value greater than
    /// 3 and 10 rows under 3; a <see cref="DoubleMetric"/> would be returned with 0.33 value
    /// </summary>
    public sealed class Compliance : StandardScanShareableAnalyzer<NumMatchesAndCount>, IFilterableAnalyzer
    {
        /// <summary>
        /// SQL-Like predicate to apply per row <see cref="Functions.Expr"/>
        /// </summary>
        public readonly Column Predicate;

        /// <summary>
        /// Initializes a new instance of the <see cref="Compliance"/> class.
        /// </summary>
        /// <param name="instance">Unlike other column analyzers (e.g completeness) this analyzer can not
        ///                      infer to the metric instance name from column name.
        ///                      Also the constraint given here can be referring to multiple columns,
        ///                       so metric instance name should be provided,
        ///                      describing what the analysis being done for.</param>
        /// <param name="predicate">SQL-predicate to apply per row.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Functions.Expr"/>.</param>
        public Compliance(string instance, Column predicate, Option<string> where) : base("Compliance", instance,
            MetricEntity.Column, Option<string>.None, where)
        {
            Predicate = predicate;
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.AggregationFunctions"/>
        public override IEnumerable<Column> AggregationFunctions()
        {
            Column summation = Sum(AnalyzersExt.ConditionalSelection(Predicate, Where).Cast("int"));

            return new[] { summation, AnalyzersExt.ConditionalCount(Where) };
        }

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.FromAggregationResult"/>
        protected override Option<NumMatchesAndCount> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset, () =>
                new NumMatchesAndCount(result.GetAs<int>(offset), result.GetAs<int>(offset + 1)), 2);

        /// <inheritdoc cref="ScanShareableAnalyzer{S,M}.ToString"/>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb
                .Append(GetType().Name)
                .Append("(")
                .Append(Instance)
                .Append(",")
                .Append(Predicate)
                .Append(",")
                .Append(Where.GetOrElse("None"))
                .Append(")");

            return sb.ToString();
        }
    }
}
