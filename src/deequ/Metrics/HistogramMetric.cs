using System.Collections.Generic;
using System.Linq;
using deequ.Util;

namespace deequ.Metrics
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
            : base(MetricEntity.Column, "Histogram", column, value)
        {
            Column = column;
            Value = value;
        }

        public virtual IEnumerable<DoubleMetric> Flatten()
        {
            if (!Value.IsSuccess)
            {
                return new[] { new DoubleMetric(MetricEntity, $"{Name}.bins", Instance, new Try<double>(Value.Failure.Value)) };
            }

            DoubleMetric[] numberOfBins =
            {
                new DoubleMetric(MetricEntity,
                    $"{Name}.bins", Instance, Value.Get().NumberOfBins)
            };

            IEnumerable<DoubleMetric> details = Value
                .Get()
                .Values
                .SelectMany(element =>
                {
                    (string key, DistributionValue value) = element;
                    return new[]
                    {
                        new DoubleMetric(MetricEntity, $"{Name}.abs.{key}", Instance, value.Absolute),
                        new DoubleMetric(MetricEntity, $"{Name}.ratio.{key}", Instance, value.Ratio)
                    };
                });

            return numberOfBins.Concat(details);
        }
    }
}
