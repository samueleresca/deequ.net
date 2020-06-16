using System;
using System.Collections.Generic;

namespace xdeequ.Util
{
    public struct Option<T>
    {
        public static readonly Option<T> None = new Option<T>();

        public Option(T value)
        {
            Value = value;
            HasValue = true;
        }

        public bool HasValue { get; }

        public T Value { get; }

        public static implicit operator Option<T>(T value)
        {
            return new Option<T>(value);
        }

        public T GetOrElse(T fallbackValue)
        {
            return HasValue ? Value : fallbackValue;
        }

        public Option<TNew> Select<TNew>(Func<T, TNew> selector)
        {
            if (!HasValue)
                return Option<TNew>.None;

            return selector(Value);
        }

        public bool Equals(Option<T> other)
        {
            return HasValue == other.HasValue && EqualityComparer<T>.Default.Equals(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            return obj is Option<T> && Equals((Option<T>) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (EqualityComparer<T>.Default.GetHashCode(Value) * 397) ^ HasValue.GetHashCode();
            }
        }

        public override string ToString()
        {
            return HasValue ? $"Some<{Value}>" : "None";
        }

        public void OnSuccess(Action<T> action)
        {
            if (HasValue)
                action(Value);
        }
    }
}