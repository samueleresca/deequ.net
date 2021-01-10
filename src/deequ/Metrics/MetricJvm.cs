using System;
using deequ.Util;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Metrics
{

    public class ExceptionJvm : IJvmObjectReferenceProvider
    {
        public JvmObjectReference Reference { get; }

        private JvmObjectReference _jvmObject;

        public ExceptionJvm(JvmObjectReference reference)
        {
            _jvmObject = reference;
        }
        public override string ToString() => (string) _jvmObject.Invoke("toString");
        /// <summary>
        ///
        /// </summary>
        /// <param name="jvmObjectReference"></param>
        /// <returns></returns>
        public static implicit operator ExceptionJvm(JvmObjectReference jvmObjectReference)
        {
            return new ExceptionJvm(jvmObjectReference);
        }
    }

    public class MetricJvm<T> : IMetric, IJvmObjectReferenceProvider
    {

        protected readonly JvmObjectReference _jvmObject;
        /// <summary>
        /// The value of the metric wrapped in a Try monad <see cref="Try{T}"/>.
        /// </summary>
        public TryJvm<T> Value () => new TryJvm<T>((JvmObjectReference)_jvmObject.Invoke("value"));

        /// <summary>
        /// The metric entity.
        /// </summary>
        public object MetricEntity() => _jvmObject.Invoke("entity");

        /// <inheritdoc cref="IMetric.Name"/>
        public string Name() =>  (string) _jvmObject.Invoke("name");

        public string Instance() => (string) _jvmObject.Invoke("instance");

        /// <inheritdoc cref="IMetric.IsSuccess"/>
        public bool IsSuccess() => Value().IsSuccess();

        /// <inheritdoc cref="IMetric.Exception"/>
        public TryJvm<ExceptionJvm> Exception()
        {
            if (IsSuccess()) return null;

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
    }
}
