using System;
using xdeequ.Analyzers.Runners;

namespace xdeequ.Extensions
{
    public static class ExceptionExt
    {
        public static MetricCalculationException WrapIfNecessary(Exception e)
        {
            return new MetricCalculationRuntimeException(e.Message);
        }
    }
}