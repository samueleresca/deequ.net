using System;

namespace xdeequ.Analyzers.Runners
{
    public abstract class MetricCalculationException : Exception
    {
        public string Message { get; set; }

        public MetricCalculationException(string message)
        {
            Message = message;
        }
    }

    public class MetricCalculationRuntimeException : MetricCalculationException
    {
        public MetricCalculationRuntimeException(string message) : base(message)
        {
        }
    }

    public class EmptyStateException : MetricCalculationRuntimeException
    {
        public EmptyStateException(string message) : base(message)
        {
        }
    }
}