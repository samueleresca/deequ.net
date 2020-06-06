namespace xdeequ.Analyzers.States
{
    public abstract class State<T> where T : State<T>
    {
        public abstract T Sum(T other);
    }
}