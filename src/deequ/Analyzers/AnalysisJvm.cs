using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers.Runners;
using deequ.Interop.Utils;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using StorageLevel = deequ.Analyzers.Runners.StorageLevel;

namespace deequ.Analyzers
{
    public class AnalysisJvm : IJvmObjectReferenceProvider
    {
        private JvmObjectReference _reference;

        public AnalysisJvm(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {

            var sequence = new SeqJvm(analyzers.Select(x=> ((AnalyzerJvmBase) x).Reference));

            JvmObjectReference reference = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallStaticJavaMethod("com.amazon.deequ.analyzers.Analysis",
                    "apply", sequence);

            _reference = reference;
        }

        public AnalysisJvm()
        {

            var defaultArg = SparkEnvironment.JvmBridge
                .CallStaticJavaMethod("com.amazon.deequ.analyzers.Analysis",
                    "apply$default$1");

            JvmObjectReference reference = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallStaticJavaMethod("com.amazon.deequ.analyzers.Analysis",
                    "apply", defaultArg);

            _reference = reference;
        }

        protected AnalysisJvm(JvmObjectReference reference)
        {
            _reference = reference;
        }

        public AnalysisJvm AddAnalyzer(IAnalyzer<IMetric> analyzer)
        {
            var analyzerRef = ((AnalyzerJvmBase) analyzer).Reference;

            JvmObjectReference reference = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallNonStaticJavaMethod(_reference, "addAnalyzer", analyzerRef);

            return  new AnalysisJvm(reference);
        }

        public AnalysisJvm AddAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {
            var sequence = new SeqJvm(analyzers.Select(x=> ((AnalyzerJvmBase) x).Reference).ToArray());

            JvmObjectReference reference = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallNonStaticJavaMethod(_reference, "addAnalyzers", sequence.Reference);

            return new AnalysisJvm(reference);
        }



        public JvmObjectReference Reference => _reference;

        /// <summary>
        /// Compute the metrics from the analyzers configured in the analysis.
        /// </summary>
        /// <param name="data">data on which to operate <see cref="DataFrame"/></param>
        /// <param name="aggregateWith">load existing states for the configured analyzers and aggregate them <see cref="IStateLoader"/>.</param>
        /// <param name="saveStateWith">persist resulting states for the configured analyzers <see cref="IStatePersister"/>.</param>
        /// <param name="storageLevelOfGroupedDataForMultiplePasses">caching level for grouped data that must be accessed multiple times (use StorageLevel.NONE to completely disable caching) <see cref="Runners.StorageLevel"/></param>
        /// <returns>The analyzer context object resulting from the computation <see cref="AnalyzerContext"/>. </returns>
        public AnalyzerContext Run(DataFrame data,
            Option<IStateLoader> aggregateWith = default,
            Option<IStatePersister> saveStateWith = default,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK)
        {
            return AnalysisRunnerJvm.Run(data, this, aggregateWith, saveStateWith,
                storageLevelOfGroupedDataForMultiplePasses);
        }
    }
}
