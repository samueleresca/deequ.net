using System;
using deequ.Metrics;

namespace deequ.Analyzers.Runners
{
    internal class SimpleMetricOutput
    {
        public SimpleMetricOutput(DoubleMetric doubleMetric)
        {
            Entity = Enum.GetName(typeof(Entity), doubleMetric.Entity);
            Instance = doubleMetric.Instance;
            Name = doubleMetric.Name;
            Value = doubleMetric.Value.Get();
        }

        public SimpleMetricOutput()
        {
        }

        public string Entity { get; set; }
        public string Instance { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
    }
}
