using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers.Runners;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using StorageLevel = deequ.Analyzers.Runners.StorageLevel;

namespace deequ.Analyzers
{
    /// <summary>
    /// Defines a set of analyzers to run on data.
    /// </summary>
    public class Analysis
    {
        /// <summary>
        /// List of analyzers that are compose the Analysis.
        /// </summary>
        public IEnumerable<IAnalyzer<IMetric>> Analyzers;

        /// <summary>
        /// Initializes a new instance of the <see cref="Analysis"/> class.
        /// </summary>
        /// <param name="analyzers">The list of analyzers <see cref="IAnalyzer{M}"/> to use in the analysis.</param>
        public Analysis(IEnumerable<IAnalyzer<IMetric>> analyzers) => Analyzers = analyzers;

        /// <summary>
        /// Initializes a new instance of the <see cref="Analysis"/> class.
        /// </summary>
        public Analysis() => Analyzers = Enumerable.Empty<IAnalyzer<IMetric>>();

        /// <summary>
        /// Add an analyzer to the analysis.
        /// </summary>
        /// <param name="analyzer">The analyzer to add <see cref="IAnalyzer{M}"/>.</param>
        /// <returns>The new analysis instance.</returns>
        public Analysis AddAnalyzer(IAnalyzer<IMetric> analyzer) => new Analysis(Analyzers.Append(analyzer));

        /// <summary>
        /// Add a list of analyzers to the analysis
        /// </summary>
        /// <param name="analyzers">The analyzer to add <see cref="IAnalyzer{M}"/>.</param>
        /// <returns>The new analysis instance.</returns>
        public Analysis AddAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers) =>
            new Analysis(Analyzers.Concat(analyzers));

        /// <summary>
        /// Compute the metrics from the analyzers configured in the analysis.
        /// </summary>
        /// <param name="data">data on which to operate <see cref="DataFrame"/></param>
        /// <param name="aggregateWith">load existing states for the configured analyzers and aggregate them <see cref="IStateLoader"/>.</param>
        /// <param name="saveStateWith">persist resulting states for the configured analyzers <see cref="IStatePersister"/>.</param>
        /// <param name="storageLevelOfGroupedDataForMultiplePasses">caching level for grouped data that must be accessed multiple times (use StorageLevel.NONE to completely disable caching) <see cref="StorageLevel"/></param>
        /// <returns>The analyzer context object resulting from the computation <see cref="AnalyzerContext"/>. </returns>
        public AnalyzerContext Run(DataFrame data,
            Option<IStateLoader> aggregateWith = default,
            Option<IStatePersister> saveStateWith = default,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK)
        {
            return AnalysisRunnerJvm.Run(data, null, aggregateWith, saveStateWith,
                storageLevelOfGroupedDataForMultiplePasses);
        }
    }
}
