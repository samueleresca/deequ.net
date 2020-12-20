using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Util;

namespace deequ.Metrics
{
    /// <summary>
    /// Describes the type of entity of a metric.
    /// </summary>
    public enum MetricEntity
    {
        /// <summary>
        /// A dataset bind metric
        /// </summary>
        Dataset = 0,
        /// <summary>
        /// A column bind metric
        /// </summary>
        Column = 1,
        /// <summary>
        /// A multiple column bind metric
        /// </summary>
        Multicolumn = 2
    }

    /// <summary>
    /// A common interface for all data quality metrics.
    /// </summary>
    public interface IMetric
    {
        /// <summary>
        /// The name of the metric.
        /// </summary>
        public string Name { get; }
        /// <summary>
        /// The instance of the metric.
        /// </summary>
        public string Instance { get; }

        /// <summary>
        /// Check if a metric is successful/
        /// </summary>
        /// <returns>Returns true if the metric is successful, otherwise false.</returns>
        public bool IsSuccess();

        /// <summary>
        /// Wrap and returns the metric exceptions.
        /// </summary>
        /// <returns>If present, returns the exception of the metric. Otherwise None <see cref="Option{T}.None"/></returns>
        public Option<Exception> Exception();
    }


    /// <summary>
    /// A class that describes a generic metric
    /// </summary>
    public abstract class Metric<T> : IMetric
    {
        /// <summary>
        /// The value of the metric wrapped in a Try monad <see cref="Try{T}"/>.
        /// </summary>
        public Try<T> Value;

        /// <summary>
        /// The metric entity.
        /// </summary>
        public MetricEntity MetricEntity { get; }

        /// <inheritdoc cref="IMetric.Name"/>
        public string Name { get; }

        /// <inheritdoc cref="IMetric.Instance"/>
        public string Instance { get; }

        /// <inheritdoc cref="IMetric.IsSuccess"/>
        public bool IsSuccess() => Value.IsSuccess;

        /// <inheritdoc cref="IMetric.Exception"/>
        public Option<Exception> Exception() => Value.Failure;

        /// <summary>
        /// Initializes a new metric <see cref="Metric{T}"/>.
        /// </summary>
        /// <param name="metricEntity">The entity type of the metric <see cref="MetricEntity"/></param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="instance">The instance of the metric.</param>
        /// <param name="value">The value of the metric, wrapped in a Try monad <see cref="Try{T}"/></param>
        protected Metric(MetricEntity metricEntity, string name, string instance, Try<T> value)
        {
            MetricEntity = metricEntity;
            Name = name;
            Instance = instance;
            Value = value;
        }

        /// <summary>
        /// Return the metric as IEnumerable
        /// </summary>
        /// <returns>The IEnumerable representing the metric <see cref="DoubleMetric"/>. </returns>
        public virtual IEnumerable<Metric<T>> Flatten() => new[] { this }.AsEnumerable();
    }

    /// <summary>
    /// Common class for all data quality metrics where the value is double
    /// </summary>
    public class DoubleMetric : Metric<double>, IEquatable<DoubleMetric>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DoubleMetric"/> class.
        /// </summary>
        /// <param name="metricEntity">The entity type of the metric <see cref="MetricEntity"/></param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="instance">The instance of the metric.</param>
        /// <param name="value">The value of the metric, wrapped in a Try monad <see cref="Try{T}"/></param>
        public DoubleMetric(MetricEntity metricEntity, string name, string instance, Try<double> value)
            : base(metricEntity, name, instance, value)
        {
        }

        /// <summary>
        /// Equality method of two <see cref="DoubleMetric"/> instances.
        /// </summary>
        /// <param name="other">The other <see cref="DoubleMetric"/> instance.</param>
        /// <returns>true if the equality is satisfied, otherwise false.</returns>
        public bool Equals(DoubleMetric other) =>
            other != null
            && Name == other.Name && Instance == other.Instance && MetricEntity == other.MetricEntity && Value.IsSuccess == other.Value.IsSuccess
            && Value.GetOrElse(() => 0).Get() == other.Value.GetOrElse(() => 0).Get();

        /// <summary>
        /// Initializes a new instance of the <see cref="DoubleMetric"/> class.
        /// </summary>
        /// <param name="metricEntity">The entity type of the metric <see cref="MetricEntity"/></param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="instance">The instance of the metric.</param>
        /// <param name="value">The value of the metric, wrapped in a Try monad <see cref="Try{T}"/></param>
        /// <returns>The initialized instance.</returns>
        public static DoubleMetric Create(MetricEntity metricEntity, string name, string instance, Try<double> value) =>
            new DoubleMetric(metricEntity, name, instance, value);


        /// <summary>
        /// Overrides the equality comparison between two <see cref="DoubleMetric"/> types.
        /// </summary>
        /// <param name="obj">An object representing a <see cref="DoubleMetric"/>.</param>
        /// <returns>true if the equality is satisfied, otherwise false.</returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != GetType())
            {
                return false;
            }

            return Equals((DoubleMetric)obj);
        }
    }
}
