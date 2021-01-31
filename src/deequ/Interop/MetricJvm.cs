using System;
using deequ.Interop.Utils;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Interop
{
    public class MetricJvm<T> : IMetric, IJvmObjectReferenceProvider
    {

        protected readonly JvmObjectReference _jvmObject;
        /// <summary>
        /// The value of the metric wrapped in a Try monad <see cref="Try{T}"/>.
        /// </summary>
        public TryJvm<T> Value  => new TryJvm<T>((JvmObjectReference)_jvmObject.Invoke("value"));

        /// <summary>
        /// The metric entity.
        /// </summary>
        public MetricEntity MetricEntity
        {
            get
            {
                var enumEntity = (JvmObjectReference) _jvmObject.Invoke("entity");
                MetricEntity value = (MetricEntity)Enum.Parse(typeof(MetricEntity),
                    (string)enumEntity.Invoke("toString"));
                return value;
            }
        }

        /// <inheritdoc cref="IMetric.Name"/>
        public string Name =>  (string) _jvmObject.Invoke("name");

        public string Instance => (string) _jvmObject.Invoke("instance");

        /// <inheritdoc cref="IMetric.IsSuccess"/>
        public bool IsSuccess => Value.IsSuccess();

        /// <inheritdoc cref="IMetric.Exception"/>
        public TryJvm<ExceptionJvm> Exception()
        {
            if (IsSuccess) return null;

            JvmObjectReference reference = (JvmObjectReference)_jvmObject.Invoke("value");
            JvmObjectReference exception = (JvmObjectReference) reference.Invoke("failed");
            return new TryJvm<ExceptionJvm>(exception);
        }

        /// <summary>
        /// Initializes a new metric <see cref="Metric{T}"/>.
        /// </summary>
        /// <param name="metricEntity">The entity type of the metric <see cref="MetricEntity"/></param>
        /// <param name="name">The name of the metric.</param>
        /// <param name="instance">The instance of the metric.</param>
        /// <param name="value">The value of the metric, wrapped in a Try monad <see cref="Try{T}"/></param>
        public MetricJvm(JvmObjectReference jvmObjectReference)
        {
            _jvmObject = jvmObjectReference;
        }



        public JvmObjectReference Reference => _jvmObject;


        public override string ToString() => (string) _jvmObject.Invoke("toString");
        bool IMetric.IsSuccess() => throw new NotImplementedException();
    }
}
