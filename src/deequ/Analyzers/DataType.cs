using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers.Catalyst;
using xdeequ.Analyzers.Runners;
using xdeequ.Analyzers.States;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Analyzers
{
    public enum DataTypeInstances
    {
        Unknown = 0,
        Fractional = 1,
        Integral = 2,
        Boolean = 3,
        String = 4
    }


    public class DataTypeHistogram : State<DataTypeHistogram>, IState
    {
        private const int SIZE_IN_BITES = 5;
        private const int NULL_POS = 0;
        private const int FRATIONAL_POS = 1;
        private const int INTEGRAL_POS = 2;
        private const int BOOLEAN_POS = 3;
        private const int STRING_POS = 4;

        public DataTypeHistogram(long nonNull, long numFractional, long numIntegral, long numBoolean, long numString)
        {
            NonNull = nonNull;
            NumFractional = numFractional;
            NumIntegral = numIntegral;
            NumBoolean = numBoolean;
            NumString = numString;
        }

        public long NonNull { get; set; }
        public long NumFractional { get; set; }
        public long NumIntegral { get; set; }
        public long NumBoolean { get; set; }
        public long NumString { get; set; }

        public IState Sum(IState other) => throw new NotImplementedException();

        public override DataTypeHistogram Sum(DataTypeHistogram other) =>
            new DataTypeHistogram(NonNull + other.NonNull, NumFractional + other.NumFractional,
                NumIntegral + other.NumIntegral, NumBoolean + other.NumBoolean, NumString + other.NumString);

        public static DataTypeHistogram FromArray(int[] typesCount)
        {
            if (typesCount.Length != SIZE_IN_BITES)
            {
                throw new Exception();
            }

            int numNull = typesCount[NULL_POS];
            int numFractional = typesCount[FRATIONAL_POS];
            int numIntegral = typesCount[INTEGRAL_POS];
            int numBoolean = typesCount[BOOLEAN_POS];
            int numString = typesCount[STRING_POS];

            return new DataTypeHistogram(numNull, numFractional, numIntegral, numBoolean, numString);
        }

        public static Distribution ToDistribution(DataTypeHistogram hist)
        {
            long totalObservations =
                hist.NonNull + hist.NumString + hist.NumBoolean + hist.NumIntegral + hist.NumFractional;

            return new Distribution(
                new Dictionary<string, DistributionValue>
                {
                    {
                        DataTypeInstances.Unknown.ToString(),
                        new DistributionValue(hist.NonNull, (double)hist.NonNull / totalObservations)
                    },
                    {
                        DataTypeInstances.Fractional.ToString(),
                        new DistributionValue(hist.NumFractional, (double)hist.NumFractional / totalObservations)
                    },
                    {
                        DataTypeInstances.Integral.ToString(),
                        new DistributionValue(hist.NumIntegral, (double)hist.NumIntegral / totalObservations)
                    },
                    {
                        DataTypeInstances.Boolean.ToString(),
                        new DistributionValue(hist.NumBoolean, (double)hist.NumBoolean / totalObservations)
                    },
                    {
                        DataTypeInstances.String.ToString(),
                        new DistributionValue(hist.NumString, (double)hist.NumString / totalObservations)
                    }
                }, 5);
        }
    }

    public class DataType : ScanShareableAnalyzer<DataTypeHistogram, HistogramMetric>, IFilterableAnalyzer,
        IAnalyzer<HistogramMetric>
    {
        public string Column;
        public Option<string> Where;

        public DataType(string column, Option<string> where)
        {
            Column = column;
            Where = where;
        }

        public override HistogramMetric ToFailureMetric(Exception e) =>
            new HistogramMetric(Column, new Try<Distribution>(e));

        public override IEnumerable<Action<StructType>> Preconditions() =>
            new[] {AnalyzersExt.HasColumn(Column), AnalyzersExt.IsNotNested(Column)}.Concat(base.Preconditions());

        public Option<string> FilterCondition() => Where;

        public override HistogramMetric ComputeMetricFrom(Option<DataTypeHistogram> state)
        {
            //TODO: Empty message as exception
            if (!state.HasValue)
            {
                return ToFailureMetric(new EmptyStateException(string.Empty));
            }

            return new HistogramMetric(Column, new Try<Distribution>(DataTypeHistogram.ToDistribution(state.Value)));
        }


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

        public override IEnumerable<Column> AggregationFunctions() =>
            new[] {AnalyzersExt.ConditionalSelection(Column, Where)};

        public override Option<DataTypeHistogram> FromAggregationResult(Row result, int offset) =>
            AnalyzersExt.IfNoNullsIn(result, offset,
                () => { return DataTypeHistogram.FromArray(result.Values.Select(x => (int)x).ToArray()); });
    }
}