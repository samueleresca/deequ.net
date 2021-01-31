using deequ.Metrics;

namespace deequ.Analyzers
{
    /// <summary>
    /// A abstract class that represents all the analyzers which generates metrics from states computed on data frames.
    /// </summary>
    /// <typeparam name="M">The output <see cref="Metric{T}"/> of the analyzer.</typeparam>
    public interface IAnalyzer<out M> where M : IMetric
    {

    }
}
