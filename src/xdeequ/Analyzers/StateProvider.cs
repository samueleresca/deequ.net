using xdeequ.Analyzers.States;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public interface IStatePersister
    {
        S Persist<S, M>(Option<Analyzer<S, M>> analyzer, Option<S> state) where S : State<S>;
        S Persist<S, M>(Option<Analyzer<S, M>> analyzer, S state) where S : State<S>;
    }

    public interface IStateLoader
    {
        Option<S> Load<S, M>(Option<Analyzer<S, M>> analyzer) where S : State<S>;
    }
}