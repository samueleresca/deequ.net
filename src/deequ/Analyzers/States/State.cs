using System;

namespace deequ.Analyzers.States
{
    public interface IState
    {
    }

    public abstract class State<T> : IState where T : State<T>
    {
        public virtual T Sum(T other) => throw new NotImplementedException();
    }
}
