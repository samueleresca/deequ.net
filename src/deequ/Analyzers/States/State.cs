using System;

namespace deequ.Analyzers.States
{
    /// <summary>
    /// Represents a State
    /// </summary>
    public interface IState
    {
    }
    /// <summary>
    /// Represents a generic State.
    /// </summary>
    /// <typeparam name="T">The type of state wrapped by the object.</typeparam>
    public abstract class State<T> : IState
    {
        /// <summary>
        /// Defines the sum of two states of the same type.
        /// </summary>
        /// <param name="other">The value to sum to the state.</param>
        /// <returns></returns>
        public virtual T Sum(T other) => throw new NotImplementedException();
    }
}
