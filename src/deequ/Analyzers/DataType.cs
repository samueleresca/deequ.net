using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using deequ.Analyzers.Catalyst;
using deequ.Analyzers.Runners;
using deequ.Analyzers.States;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Analyzers
{
    /// <summary>
    /// Data type instances
    /// </summary>
    internal enum DataTypeInstances
    {
        Unknown = 0,
        Fractional = 1,
        Integral = 2,
        Boolean = 3,
        String = 4
    }


    /// <summary>
    /// Represents the data type state.
    /// </summary>
    public class DataTypeHistogram : State<DataTypeHistogram>
    {


        private const int SIZE_IN_BITES = 5;
        private const int NULL_POS = 0;
        private const int FRATIONAL_POS = 1;
        private const int INTEGRAL_POS = 2;
        private const int BOOLEAN_POS = 3;
        private const int STRING_POS = 4;

        /// <summary>
        /// Initializes a new instance of the <see cref="DataTypeHistogram"/> class.
        /// </summary>
        /// <param name="nonNull">Count of non null values.</param>
        /// <param name="numFractional">Count of the fractional values.</param>
        /// <param name="numIntegral">Count of the integral values.</param>
        /// <param name="numBoolean">Count of the boolean values.</param>
        /// <param name="numString">Count of the string values.</param>
        public DataTypeHistogram(long nonNull, long numFractional, long numIntegral, long numBoolean, long numString)
        {
            NonNull = nonNull;
            NumFractional = numFractional;
            NumIntegral = numIntegral;
            NumBoolean = numBoolean;
            NumString = numString;
        }

        /// <summary>
        /// Represents the count of non-null values.
        /// </summary>
        public long NonNull { get; }
        /// <summary>
        /// Represents the count of fractional values.
        /// </summary>
        public long NumFractional { get; }
        /// <summary>
        /// Represents the count of integral values.
        /// </summary>
        public long NumIntegral { get; }
        /// <summary>
        /// Represents the count of bool values.
        /// </summary>
        public long NumBoolean { get; }
        /// <summary>
        /// Represents the count of string values.
        /// </summary>
        public long NumString { get; }

        /// <inheritdoc cref="State{T}.Sum"/>
        public IState Sum(IState other) => throw new NotImplementedException();

        /// <inheritdoc cref="State{T}.Sum"/>
        public override DataTypeHistogram Sum(DataTypeHistogram other) =>
            new DataTypeHistogram(NonNull + other.NonNull, NumFractional + other.NumFractional,
                NumIntegral + other.NumIntegral, NumBoolean + other.NumBoolean, NumString + other.NumString);

        /// <summary>
        /// Converts an array to a <see cref="DataTypeHistogram"/>.
        /// </summary>
        /// <param name="typesCount">Array that represents the counts of the different types.</param>
        /// <returns>An instance of the <see cref="DataTypeHistogram"/> instance with the corresponding values.</returns>
        /// <exception cref="Exception">The SIZE_IN_BITES doesn't correspond to the Length of the array.</exception>
        public static DataTypeHistogram FromArray(int[] typesCount)
        {
            if (typesCount.Length != SIZE_IN_BITES)
            {
                throw new Exception("The SIZE_IN_BITES doesn't correspond to the Length of the array.");
            }

            int numNull = typesCount[NULL_POS];
            int numFractional = typesCount[FRATIONAL_POS];
            int numIntegral = typesCount[INTEGRAL_POS];
            int numBoolean = typesCount[BOOLEAN_POS];
            int numString = typesCount[STRING_POS];

            return new DataTypeHistogram(numNull, numFractional, numIntegral, numBoolean, numString);
        }

        /// <summary>
        /// Converts the a <see cref="DataTypeHistogram"/> instance to a <see cref="Distribution"/> instance.
        /// </summary>
        /// <param name="value">The <see cref="DataTypeHistogram"/> to convert.</param>
        /// <returns>The <see cref="Distribution"/> instance.</returns>
        public static Distribution ToDistribution(DataTypeHistogram value)
        {
            long totalObservations =
                value.NonNull + value.NumString + value.NumBoolean + value.NumIntegral + value.NumFractional;

            return new Distribution(
                new Dictionary<string, DistributionValue>
                {
                    {
                        DataTypeInstances.Unknown.ToString(),
                        new DistributionValue(value.NonNull, (double)value.NonNull / totalObservations)
                    },
                    {
                        DataTypeInstances.Fractional.ToString(),
                        new DistributionValue(value.NumFractional, (double)value.NumFractional / totalObservations)
                    },
                    {
                        DataTypeInstances.Integral.ToString(),
                        new DistributionValue(value.NumIntegral, (double)value.NumIntegral / totalObservations)
                    },
                    {
                        DataTypeInstances.Boolean.ToString(),
                        new DistributionValue(value.NumBoolean, (double)value.NumBoolean / totalObservations)
                    },
                    {
                        DataTypeInstances.String.ToString(),
                        new DistributionValue(value.NumString, (double)value.NumString / totalObservations)
                    }
                }, 5);
        }
    }

    /// <summary>
    /// Data type analyzers, analyzes the data type of the target column.
    /// </summary>
    public sealed class DataType : ScanShareableAnalyzer<DataTypeHistogram, HistogramMetric>, IFilterableAnalyzer
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="DataType"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Functions.Expr"/>.</param>
        public DataType(string column, Option<string> where)
        {
            Column = column;
            Where = where;
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.ToFailureMetric"/>.
        public override HistogramMetric ToFailureMetric(Exception e) =>
            new HistogramMetric(Column.Value, new Try<Distribution>(e));

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.Preconditions"/>.
        public override IEnumerable<Action<StructType>> Preconditions() =>
            new[] { AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNotNested(Column) }.Concat(base.Preconditions());

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.ComputeMetricFrom"/>.
        public override HistogramMetric ComputeMetricFrom(Option<DataTypeHistogram> state)
        {
            //TODO: Empty message as exception
            if (!state.HasValue)
            {
                return ToFailureMetric(new EmptyStateException(string.Empty));
            }

            return new HistogramMetric(Column.Value, new Try<Distribution>(DataTypeHistogram.ToDistribution(state.Value)));
        }
        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.ComputeStateFrom"/>.
        public override Option<DataTypeHistogram> ComputeStateFrom(DataFrame dataFrame)
        {
            StatefulDataType statefulDataType = new StatefulDataType();
            IEnumerable<Column> aggregations = AggregationFunctions();
            Func<Column, Column> arrayDataTypeCountUdf = Udf<string, int[]>(value => statefulDataType.Update(value));

            string[] listOfColumns = statefulDataType.ColumnNames();
            string aggregatedColumn = statefulDataType.GetAggregatedColumn();

            Row result = dataFrame
                .WithColumn(aggregatedColumn, arrayDataTypeCountUdf(aggregations.First().Cast("string")))
                .WithColumn(listOfColumns[0], Column(aggregatedColumn).GetItem(0))
                .WithColumn(listOfColumns[1], Column(aggregatedColumn).GetItem(1))
                .WithColumn(listOfColumns[2], Column(aggregatedColumn).GetItem(2))
                .WithColumn(listOfColumns[3], Column(aggregatedColumn).GetItem(3))
                .WithColumn(listOfColumns[4], Column(aggregatedColumn).GetItem(4))
                .GroupBy()
                .Sum(listOfColumns)
                .Collect()
                .FirstOrDefault();

            return FromAggregationResult(result, 0);
        }

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.AggregationFunctions"/>.
        public override IEnumerable<Column> AggregationFunctions() =>
            new[] { AnalyzersExt.ConditionalSelection(Column, Where) };

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.FromAggregationResult"/>.
        protected override Option<DataTypeHistogram> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => { return DataTypeHistogram.FromArray(result.Values.Select(value => (int)value).ToArray()); });

        /// <inheritdoc cref="StandardScanShareableAnalyzer{S}.ToString"/>.
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
