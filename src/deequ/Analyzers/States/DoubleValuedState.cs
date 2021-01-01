namespace deequ.Analyzers.States
{
    /// <summary>
    /// A state which produced double value metric.
    /// </summary>
    /// <typeparam name="S">The state of type <see cref="DoubleValuedState{S}"/></typeparam>
    public abstract class DoubleValuedState<S> : State<S> where S : DoubleValuedState<S>
    {
        /// <summary>
        /// Get the metric value
        /// </summary>
        /// <returns>The value of the metric.</returns>
        public abstract double GetMetricValue();
    }
}
