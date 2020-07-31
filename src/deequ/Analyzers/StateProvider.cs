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
        S Persist<S>(Option<IAnalyzer<IMetric>> analyzer, Option<S> state) where S : IState;
        S Persist<S>(Option<IAnalyzer<IMetric>> analyzer, S state) where S : IState;
    }

    public interface IStateLoader
    {
        Option<S> Load<S>(Option<IAnalyzer<IMetric>> analyzer) where S : IState;
    }

    public class InMemoryStateProvider : IStateLoader, IStatePersister
    {
        private readonly ConcurrentDictionary<IAnalyzer<IMetric>, IState> statesByAnalyzer
            = new ConcurrentDictionary<IAnalyzer<IMetric>, IState>();

        public Option<S> Load<S>(Option<IAnalyzer<IMetric>> analyzer) where S : IState => !analyzer.HasValue
            ? Option<S>.None
            : new Option<S>((S)statesByAnalyzer[analyzer.Value]);

        public S Persist<S>(Option<IAnalyzer<IMetric>> analyzer, Option<S> state) where S : IState
        {
            if (!analyzer.HasValue)
            {
                return (S)(IState)null;
            }

            statesByAnalyzer[(IAnalyzer<IMetric>)analyzer.Value] = state.Value;
            return state.Value;
        }

        public S Persist<S>(Option<IAnalyzer<IMetric>> analyzer, S state) where S : IState
        {
            if (!analyzer.HasValue)
            {
                return (S)(IState)null;
            }

            statesByAnalyzer[analyzer.Value] = state;
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
