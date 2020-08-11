namespace deequ.Analyzers.States
{
    public abstract class DoubleValuedState<S> : State<S> where S : State<S>
    {
        public abstract double MetricValue();
    }
}
