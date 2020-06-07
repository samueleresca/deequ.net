using System;

namespace xdeequ.Analyzers.States
{
    public abstract class DoubleValuedState<S> : State<S> where S : State<S>
    {
        public abstract Double MetricValue();
    }
}