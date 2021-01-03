using System.Collections.Generic;
using System.Linq;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers
{
    public class AnalysisRunBuilder
    {
        /// <summary>
        ///
        /// </summary>
        public JvmObjectReference _AnalysisRunBuilder;

        public AnalysisRunBuilder(DataFrame df, IJvmBridge bridge)
        {
            _AnalysisRunBuilder = bridge
                .CallConstructor("com.amazon.deequ.analyzers.runners.AnalysisRunBuilder", df);

        }

        public AnalysisRunBuilder AddAnalyzer(IAnalyzer<IMetric> analyzer)
        {
            var analyzerBase = (AnalyzerJvmBase)analyzer;
            _AnalysisRunBuilder.Invoke("addAnalyzer", analyzerBase.Reference);
            return this;
        }

        public AnalysisRunBuilder AddAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {
            foreach (IAnalyzer<IMetric> analyzer in analyzers)
                AddAnalyzer(analyzer);

            return this;
        }

        public AnalyzerContext Run()
        {
            return new AnalyzerContext((JvmObjectReference)_AnalysisRunBuilder.Invoke("run"));
        }

        public AnalysisRunBuilder UseRepository(MetricsRepository repository)
        {
            _AnalysisRunBuilder = (JvmObjectReference) _AnalysisRunBuilder.Invoke("useRepository", repository.Reference);
            return this;
        }

        public AnalysisRunBuilder SaveOrAppendResult(ResultKey resultKey)
        {
            _AnalysisRunBuilder.Invoke("saveOrAppendResult", resultKey.Reference);
            return this;
        }


    }
}
