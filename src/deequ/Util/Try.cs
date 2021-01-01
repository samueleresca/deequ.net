using System;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Util
{
    public class Try<T>
    {
        public Try(T success) => Success = success;

        public Try(JvmObjectReference objectReference) => Success = (T)objectReference.Invoke("get");

        public Try(Exception failure) => Failure = failure;

        public bool IsSuccess => Success.HasValue;

        public Option<T> Success { get; } = Option<T>.None;

        public Option<Exception> Failure { get; } = Option<Exception>.None;

        public static implicit operator Try<T>(T value) => new Try<T>(value);

        public Try<T> Recover(Action<Exception> failureHandler)
        {
            if (Failure.HasValue)
            {
                try
                {
                    failureHandler(Failure.Value);
                    return this;
                }
                catch (Exception ex)
                {
                    return new Try<T>(ex);
                }
            }

            return this;
        }

        public static Try<T> From(Func<T> func)
        {
            try
            {
                return new Try<T>(func());
            }
            catch (Exception ex)
            {
                return new Try<T>(ex);
            }
        }

        public Try<T> OrElse(Try<T> @default)
        {
            if (Success.HasValue)
            {
                return this;
            }

            return @default;
        }

        public Try<T> GetOrElse(Func<T> fallback)
        {
            if (Success.HasValue)
            {
                return Success.Value;
            }

            try
            {
                return fallback();
            }
            catch (Exception ex)
            {
                return new Try<T>(ex);
            }
        }

        public T Get()
        {
            if (Failure.HasValue)
            {
                throw Failure.Value;
            }

            return Success.Value;
        }
    }
}
