using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Xml.Schema;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers.Runners;
using xdeequ.Analyzers.States;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;

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


    public class DataTypeHistogram : State<DataTypeHistogram>
    {
        public long NonNull { get; set; }
        public long NumFractional { get; set; }
        public long NumIntegral { get; set; }
        public long NumBoolean { get; set; }
        public long NumString { get; set; }

        const int SIZE_IN_BITES = 40;
        const int NULL_POS = 0;
        const int FRATIONAL_POS = 1;
        const int INTEGRAL_POS = 2;
        const int BOOLEAN_POS = 3;
        const int STRING_POS = 4;

        public DataTypeHistogram(long nonNull, long numFractional, long numIntegral, long numBoolean, long numString)
        {
            NonNull = nonNull;
            NumFractional = numFractional;
            NumIntegral = numIntegral;
            NumBoolean = numBoolean;
            NumString = numString;
        }

        public override DataTypeHistogram Sum(DataTypeHistogram other)
        {
            return new DataTypeHistogram(NonNull + other.NonNull, NumFractional + other.NumFractional,
                NumIntegral + other.NumIntegral, NumBoolean + other.NumBoolean, NumString + other.NumString);
        }

        public static DataTypeHistogram FromBytes(ReadOnlySpan<byte> bytes)
        {
            if (bytes.Length != SIZE_IN_BITES) throw new Exception();

            var numNull = bytes[NULL_POS];
            var numFractional = bytes[FRATIONAL_POS];
            var numIntegral = bytes[INTEGRAL_POS];
            var numBoolean = bytes[BOOLEAN_POS];
            var numString = bytes[STRING_POS];

            return new DataTypeHistogram(numNull, numFractional, numIntegral, numBoolean, numString);
        }


        public static ReadOnlySpan<byte> ToBytes(long nonNull, long numFractional, long numIntegral, long numBoolean,
            long numString)
        {
            var bytes = new MemoryStream(SIZE_IN_BITES);

            bytes.Write(BitConverter.GetBytes(nonNull));
            bytes.Write(BitConverter.GetBytes(numFractional));
            bytes.Write(BitConverter.GetBytes(numBoolean));
            bytes.Write((BitConverter.GetBytes(numString)));

            var result = new MemoryStream((int) (bytes.Length - bytes.Position));
            result.Write(bytes.GetBuffer());

            return result.GetBuffer();
        }

        public static Distribution ToDistribution(DataTypeHistogram hist)
        {
            var totalObservations =
                hist.NonNull + hist.NumString + hist.NumBoolean + hist.NumIntegral + hist.NumFractional;

            return new Distribution(new Dictionary<string, DistributionValue>
            {
                {
                    DataTypeInstances.Unknown.ToString(),
                    new DistributionValue(hist.NonNull, (double) hist.NonNull / totalObservations)
                },
                {
                    DataTypeInstances.Fractional.ToString(),
                    new DistributionValue(hist.NumFractional, (double) hist.NumFractional / totalObservations)
                },
                {
                    DataTypeInstances.Integral.ToString(),
                    new DistributionValue(hist.NumIntegral, (double) hist.NumIntegral / totalObservations)
                },
                {
                    DataTypeInstances.Boolean.ToString(),
                    new DistributionValue(hist.NumBoolean, (double) hist.NumBoolean / totalObservations)
                },
                {
                    DataTypeInstances.String.ToString(),
                    new DistributionValue(hist.NumString, (double) hist.NumString / totalObservations)
                },
            }, 5);
        }

        public static DataTypeInstances DetermineType(Distribution dist)
        {
            if (RatioOf(DataTypeInstances.Unknown, dist) == 1.0)
                return DataTypeInstances.Unknown;

            if (RatioOf(DataTypeInstances.String, dist) > 0.0 ||
                RatioOf(DataTypeInstances.Boolean, dist) > 0.0 &&
                (RatioOf(DataTypeInstances.Integral, dist) > 0.0 || RatioOf(DataTypeInstances.Fractional, dist) > 0.0))
                return DataTypeInstances.String;

            if (RatioOf(DataTypeInstances.Boolean, dist) > 0.0)
                return DataTypeInstances.Boolean;

            if (RatioOf(DataTypeInstances.Fractional, dist) > 0.0)
                return DataTypeInstances.Fractional;

            return DataTypeInstances.Integral;
        }

        private static double RatioOf(DataTypeInstances key, Distribution distribution)
        {
            return distribution.Values
                .GetValueOrDefault<string, DistributionValue>(key.ToString(), new DistributionValue(0L, 0.0))
                .Ratio;
        }
    }


    public class DataType : ScanShareableAnalyzer<DataTypeHistogram, HistogramMetric>, IFilterableAnalyzer
    {
        public string Column;
        public Option<string> Where;

        public DataType(string column, Option<string> where)
        {
            Column = column;
            Where = where;
        }

        public static DataType Create(string column) => new DataType(column, new Option<string>());

        public static DataType Create(string column, string where) => new DataType(column, where);

        public override HistogramMetric ComputeMetricFrom(Option<DataTypeHistogram> state)
        {
            //TODO: Empty message as exception
            if (!state.HasValue)
                return ToFailureMetric(new EmptyStateException(string.Empty));

            return new HistogramMetric(Column, new Try<Distribution>(DataTypeHistogram.ToDistribution(state.Value)));
        }

        public override HistogramMetric ToFailureMetric(Exception e)
            => new HistogramMetric(Column, new Try<Distribution>(e));

        public override IEnumerable<Column> AggregationFunctions()
            => new[] {StatefulExt.StatefulDataType(AnalyzersExt.ConditionalSelection(Column, Where))};

        public override Option<DataTypeHistogram> FromAggregationResult(Row result, int offset)
        {
            throw new NotImplementedException();
        }

        public Option<string> FilterCondition() => Where;
    }
}