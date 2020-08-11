using System;
using deequ.Analyzers.Runners;

namespace deequ.Extensions
{
    internal static class ExceptionExt
    {
        public static MetricCalculationException WrapIfNecessary(Exception e) =>
            new MetricCalculationRuntimeException(e.Message);
    }
}
