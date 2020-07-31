using System;
using System.Collections.Generic;
using System.Linq;
using xdeequ.Util;

namespace xdeequ.Metrics
{
    public enum Entity
    {
        Dataset = 0,
        Column = 1,
        Multicolumn = 2
    }

    public interface IMetric
    {
        public Entity Entity { get; }
        public string Name { get; }
        public string Instance { get; }

        public bool IsSuccess();

        public Option<Exception> Exception();
    }

    internal abstract class Metric<T> : IMetric
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
        public bool IsSuccess() => Value.IsSuccess;

        public Option<Exception> Exception() => Value.Failure;

        public abstract IEnumerable<DoubleMetric> Flatten();
    }

    internal class DoubleMetric : Metric<double>, IEquatable<DoubleMetric>
    {
        public DoubleMetric(Entity entity, string name, string instance, Try<double> value)
            : base(entity, name, instance, value)
        {
        }

        public bool Equals(DoubleMetric other) =>
            Name == other.Name &&
            Instance == other.Instance &&
            Entity == other.Entity &&
            Value.IsSuccess == other.Value.IsSuccess &&
            Value.GetOrElse(() => 0).Get() == other.Value.GetOrElse(() => 0).Get();

        public static DoubleMetric Create(Entity entity, string name, string instance, Try<double> value) =>
            new DoubleMetric(entity, name, instance, value);

        public override IEnumerable<DoubleMetric> Flatten() => new[] { this }.AsEnumerable();

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != GetType())
            {
                return false;
            }

            return Equals((DoubleMetric)obj);
        }
    }
}
