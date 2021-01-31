using System;
using System.Collections.Generic;
using deequ.Interop;
using deequ.Interop.Utils;
using deequ.Util;
using Microsoft.Spark.Interop.Ipc;

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
    /// Represents a distribution value.
    /// </summary>
    public class DistributionValueJvm
    {
        private readonly JvmObjectReference _jvmObject;
        /// <summary>
        /// The absolute value of the distribution.
        /// </summary>
        public long Absolute
        {
            get
            {
               return (long) _jvmObject.Invoke("absolute");
            }
        }

        /// <summary>
        /// The ratio of the distribution value.
        /// </summary>
        public double Ratio
        {
            get
            {
                return (double) _jvmObject.Invoke("ratio");
            }
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="DistributionValueJvm"/> class.
        /// </summary>
        /// <param name="absolute">The absolute value of the distribution.</param>
        /// <param name="ratio">The ratio of the distribution value.</param>
        /// <param name="jvmObjectReference"></param>
        public DistributionValueJvm(JvmObjectReference jvmObjectReference)
        {
            _jvmObject = jvmObjectReference;
        }


        /// <summary>
        ///
        /// </summary>
        /// <param name="jvmObjectReference"></param>
        /// <returns></returns>
        public static implicit operator DistributionValueJvm(JvmObjectReference jvmObjectReference)
        {
             return new DistributionValueJvm(jvmObjectReference);
        }

    }

    /// <summary>
    /// Represents a class Distribution.
    /// </summary>
    public class Distribution
    {
        /// <summary>
        /// The values of the distribution. Maps a string to a <see cref="DistributionValueJvm"/>.
        /// </summary>
        public readonly Dictionary<string, DistributionValue> Values;

        /// <summary>
        /// Number of bins in the distribution instance.
        /// </summary>
        public long NumberOfBins;

        /// <summary>
        /// Initializes a new instance of the <see cref="Distribution"/> class.
        /// </summary>
        /// <param name="values">The values of the distribution. Maps a string to a <see cref="DistributionValueJvm"/>.</param>
        /// <param name="numberOfBins">Number of bins in the distribution instance.</param>
        public Distribution(Dictionary<string, DistributionValue> values, long numberOfBins)
        {
            Values = values;
            NumberOfBins = numberOfBins;
        }

        /// <summary>
        /// Retrieves a <see cref="DistributionValueJvm"/> from the distribution.
        /// </summary>
        /// <param name="key">The key of the <see cref="DistributionValueJvm"/> you want to retrieve.</param>
        public DistributionValue this[string key]
        {
            get => Values[key];
            set => Values[key] = value;
        }
    }



    /// <summary>
    /// Represents a class Distribution.
    /// </summary>
    public class DistributionJvm
    {
        private readonly JvmObjectReference _jvmObjectReference;

        /// <summary>
        /// The values of the distribution. Maps a string to a <see cref="DistributionValueJvm"/>.
        /// </summary>
        public MapJvm Values
        {
            get
            {
                return new MapJvm((JvmObjectReference)_jvmObjectReference.Invoke("values"));
            }
        }

        /// <summary>
        /// Number of bins in the distribution instance.
        /// </summary>
        public long NumberOfBins
        {
            get
            {
                return (long) _jvmObjectReference.Invoke("numberOfBins");
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DistributionJvm"/> class.
        /// </summary>
        /// <param name="values">The values of the distribution. Maps a string to a <see cref="DistributionValueJvm"/>.</param>
        /// <param name="numberOfBins">Number of bins in the distribution instance.</param>
        public DistributionJvm(JvmObjectReference jvmObjectReference)
        {
            _jvmObjectReference = jvmObjectReference;
        }



        /// <summary>
        ///
        /// </summary>
        /// <param name="jvmObjectReference"></param>
        /// <returns></returns>
        public static implicit operator DistributionJvm(JvmObjectReference jvmObjectReference)
        {
            return new DistributionJvm(jvmObjectReference);
        }


        /// <summary>
        /// Retrieves a <see cref="DistributionValueJvm"/> from the distribution.
        /// </summary>
        /// <param name="key">The key of the <see cref="DistributionValueJvm"/> you want to retrieve.</param>
        public DistributionValueJvm this[string key]
        {
            get
            {
                OptionJvm optValue = Values.Get(key);

                if (optValue.IsEmpty())
                    return null;

                return (JvmObjectReference)optValue.Get();
            }
        }

        public string Name() => throw new NotImplementedException();

        public string Instance() => throw new NotImplementedException();

        public bool IsSuccess() => throw new NotImplementedException();

        public TryJvm<ExceptionJvm> Exception() => throw new NotImplementedException();
        public JvmObjectReference Reference => _jvmObjectReference;
    }

    /// <summary>
    /// Describes a histogram metric.
    /// </summary>
    public class HistogramMetric : Metric<Distribution>, IMetric
    {
        /// <summary>
        /// The target column of the metric.
        /// </summary>
        public Option<string> Column;

        /// <summary>
        /// Initializes a new instance of the <see cref="HistogramMetric"/> class.
        /// </summary>
        /// <param name="column">The target column of the metric.</param>
        /// <param name="value">The value of the metric <see cref="DistributionJvm"/>.</param>
        public HistogramMetric(string column, Try<Distribution> value) : base(Metrics.MetricEntity.Column, "Histogram", column, value)
        {
        }

        public HistogramMetric(MetricEntity metricEntity, string name, string instance, Try<Distribution> value)
            : base(metricEntity, name, instance, value)
        {
        }

        public bool IsSuccess() => throw new NotImplementedException();
    }
}
