using deequ.Metrics;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Interop
{
    /// <summary>
    /// Common class for all data quality metrics where the value is double
    /// </summary>
    public class DoubleMetricJvm : MetricJvm<double>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DoubleMetric"/> class.
        /// </summary>
        /// <param name="metricEntity">The entity type of the metric <see cref="MetricEntity"/></param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="instance">The instance of the metric.</param>
        /// <param name="value">The value of the metric, wrapped in a Try monad <see cref="Try{T}"/></param>
        public DoubleMetricJvm(JvmObjectReference jvmObjectReference) : base(jvmObjectReference)
        {

        }


        /// <summary>
        /// Equality method of two <see cref="DoubleMetric"/> instances.
        /// </summary>
        /// <param name="other">The other <see cref="DoubleMetric"/> instance.</param>
        /// <returns>true if the equality is satisfied, otherwise false.</returns>
        public bool Equals(DoubleMetricJvm other) => (bool) Reference.Invoke("equals", other.Reference);

        /// <summary>
        ///
        /// </summary>
        /// <param name="objectReference"></param>
        /// <returns></returns>
        public static implicit operator DoubleMetricJvm(JvmObjectReference objectReference) =>
            new DoubleMetricJvm(objectReference);


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

            return Equals((DoubleMetricJvm)obj);
        }
    }
}
