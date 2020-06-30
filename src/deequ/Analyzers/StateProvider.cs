using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public interface IStatePersister
    {
        S Persist<S, M>(Option<Analyzer<S, M>> analyzer, Option<S> state) where S : State<S>, IState;
        S Persist<S, M>(Option<Analyzer<S, M>> analyzer, S state) where S : State<S>, IState;
    }

    public interface IStateLoader
    {
        Option<S> Load<S, M>(Option<Analyzer<S, M>> analyzer) where S : State<S>, IState;
    }

    public class InMemoryStateProvider : IStateLoader, IStatePersister
    {
        private readonly ConcurrentDictionary<IAnalyzer<IMetric>, IState> statesByAnalyzer
            = new ConcurrentDictionary<IAnalyzer<IMetric>, IState>();

        public Option<S> Load<S, M>(Option<Analyzer<S, M>> analyzer) where S : State<S>, IState => !analyzer.HasValue
            ? null
            : new Option<S>((S)statesByAnalyzer[(IAnalyzer<IMetric>)analyzer.Value]);

        public S Persist<S, M>(Option<Analyzer<S, M>> analyzer, Option<S> state) where S : State<S>, IState
        {
            if (!analyzer.HasValue)
            {
                return null;
            }

            statesByAnalyzer[(IAnalyzer<IMetric>)analyzer.Value] = state.Value;
            return state.Value;
        }

        public S Persist<S, M>(Option<Analyzer<S, M>> analyzer, S state) where S : State<S>, IState
        {
            if (!analyzer.HasValue)
            {
                return null;
            }

            statesByAnalyzer[(IAnalyzer<IMetric>)analyzer.Value] = state;
            return state;
        }

        public override string ToString()
        {
            StringBuilder buffer = new StringBuilder();
            foreach (KeyValuePair<IAnalyzer<IMetric>, IState> pair in statesByAnalyzer)
            {
                buffer.Append(pair.Key);
                buffer.Append(" => ");
                buffer.Append(pair.Value);
                buffer.Append("\n");
            }

            return buffer.ToString();
        }
    }
}
