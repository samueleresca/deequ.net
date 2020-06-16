using System.Collections.Generic;
using System.Linq;
using xdeequ.Util;

namespace xdeequ.Metrics
{
    public enum Entity
    {
        DataSet = 0,
        Column = 1,
        MultiColumn = 2
    }

    public interface IMetric
    {
        public Entity Entity { get; }
        public string Name { get; }
        public string Instance { get; }
    }

    public abstract class Metric<T> : IMetric
    {
        public Try<T> Value;

        protected Metric(Entity entity, string name, string instance, Try<T> value)
        {
            Entity = entity;
            Name = name;
            Instance = instance;
            Value = value;
        }

        public Entity Entity { get; }
        public string Name { get; }
        public string Instance { get; }

        public abstract IEnumerable<DoubleMetric> Flatten();
    }

    public class DoubleMetric : Metric<double>
    {
        public DoubleMetric(Entity entity, string name, string instance, Try<double> value)
            : base(entity, name, instance, value)
        {
        }

        public static DoubleMetric Create(Entity entity, string name, string instance, Try<double> value)
        {
            return new DoubleMetric(entity, name, instance, value);
        }

        public override IEnumerable<DoubleMetric> Flatten()
        {
            return new[] {this}.AsEnumerable();
        }
    }
}