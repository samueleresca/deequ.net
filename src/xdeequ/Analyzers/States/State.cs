namespace xdeequ.Analyzers.States
{
    public interface IState
    {
        public IState Sum(IState other);
    }

    public abstract class State<T> where T : State<T>
    {
        public abstract T Sum(T other);
    }
}