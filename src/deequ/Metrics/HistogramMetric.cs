using System.Collections.Generic;
using System.Linq;
using deequ.Util;

namespace deequ.Metrics
{
    /// <summary>
    /// Represents a distribution value.
    /// </summary>
    public class DistributionValue
    {
        /// <summary>
        /// The absolute value of the distribution.
        /// </summary>
        public long Absolute;

        /// <summary>
        /// The ratio of the distribution value.
        /// </summary>
        public double Ratio;


        /// <summary>
        /// Initializes a new instance of the <see cref="DistributionValue"/> class.
        /// </summary>
        /// <param name="absolute">The absolute value of the distribution.</param>
        /// <param name="ratio">The ratio of the distribution value.</param>
        public DistributionValue(long absolute, double ratio)
        {
            Absolute = absolute;
            Ratio = ratio;
        }
    }

    /// <summary>
    /// Represents a class Distribution.
    /// </summary>
    public class Distribution
    {
        /// <summary>
        /// The values of the distribution. Maps a string to a <see cref="DistributionValue"/>.
        /// </summary>
        public readonly Dictionary<string, DistributionValue> Values;

        /// <summary>
        /// Number of bins in the distribution instance.
        /// </summary>
        public long NumberOfBins;

        /// <summary>
        /// Initializes a new instance of the <see cref="Distribution"/> class.
        /// </summary>
        /// <param name="values">The values of the distribution. Maps a string to a <see cref="DistributionValue"/>.</param>
        /// <param name="numberOfBins">Number of bins in the distribution instance.</param>
        public Distribution(Dictionary<string, DistributionValue> values, long numberOfBins)
        {
            Values = values;
            NumberOfBins = numberOfBins;
        }

        /// <summary>
        /// Retrieves a <see cref="DistributionValue"/> from the distribution.
        /// </summary>
        /// <param name="key">The key of the <see cref="DistributionValue"/> you want to retrieve.</param>
        public DistributionValue this[string key]
        {
            get => Values[key];
            set => Values[key] = value;
        }
    }

    /// <summary>
    /// Describes a histogram metric.
    /// </summary>
    public class HistogramMetric : Metric<Distribution>
    {
        /// <summary>
        /// The target column of the metric.
        /// </summary>
        public Option<string> Column;

        /// <summary>
        /// Initializes a new instance of the <see cref="HistogramMetric"/> class.
        /// </summary>
        /// <param name="column">The target column of the metric.</param>
        /// <param name="value">The value of the metric <see cref="Distribution"/>.</param>
        public HistogramMetric(string column, Try<Distribution> value)
            : base(MetricEntity.Column, "Histogram", column, value)
        {
            Column = column;
            Value = value;
        }

        /// <inheritdoc cref="Metric{T}.Flatten"/>
        public new IEnumerable<DoubleMetric> Flatten()
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
