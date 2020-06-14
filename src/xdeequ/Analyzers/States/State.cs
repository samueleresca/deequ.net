namespace xdeequ.Analyzers.States
{
    public interface IState
    {
        public IState Sum(IState other);
    }

    public abstract class State<T> where T : State<T>
    {
        public virtual T Sum(T other)
        {
            throw new System.NotImplementedException();
        }
    }
}