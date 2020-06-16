using System.Collections.Generic;
using System.Linq;
using xdeequ.Util;

namespace xdeequ.Metrics
{
    public class DistributionValue
    {
        public long Absolute;
        public double Ratio;

        public DistributionValue(long absolute, double ratio)
        {
            Absolute = absolute;
            Ratio = ratio;
        }
    }

    public class Distribution
    {
        public readonly Dictionary<string, DistributionValue> Values;
        public long NumberOfBins;

        public Distribution(Dictionary<string, DistributionValue> values, long numberOfBins)
        {
            Values = values;
            NumberOfBins = numberOfBins;
        }

        public DistributionValue this[string key]
        {
            get => Values[key];
            set => Values[key] = value;
        }
    }

    public class HistogramMetric : Metric<Distribution>
    {
        public string Column;


        public HistogramMetric(string column, Try<Distribution> value)
            : base(Entity.Column, "Histogram", column, value)
        {
            Column = column;
            Value = value;
        }

        public override IEnumerable<DoubleMetric> Flatten()
        {
            if (!Value.IsSuccess)
                return new[]
                {
                    new DoubleMetric(Entity, $"${Name}.bins", Instance, new Try<double>(Value.Failure.Value))
                };

            DoubleMetric[] numberOfBins =
            {
                new DoubleMetric(Entity,
                    $"{Name}.bins", Instance, Value.Get().NumberOfBins)
            };

            var details = Value
                .Get()
                .Values
                .SelectMany(element =>
                {
                    var (key, value) = element;
                    return new[]
                    {
                        new DoubleMetric(Entity, $"{Name}.abs.{key}", Instance, value.Absolute),
                        new DoubleMetric(Entity, $"{Name}.ratio.{key}", Instance, value.Ratio)
                    };
                });

            return numberOfBins.Concat(details);
        }
    }
}