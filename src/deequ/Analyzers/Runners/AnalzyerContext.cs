using System.Collections.Generic;
using System.Linq;
using deequ.Interop.Utils;
using deequ.Metrics;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers.Runners
{
    public class AnalyzerContext
    {
        private JvmObjectReference _jvmObjectReference;


        public AnalyzerContext(JvmObjectReference jvmObjectReference)
        {
            _jvmObjectReference = jvmObjectReference;
        }

        public AnalyzerContext(Dictionary<IAnalyzer<IMetric>, IMetric> analyzerMetrics)
        {

        }


        public DataFrame SuccessMetricsAsDataFrame(IEnumerable<IAnalyzer<IMetric>> forAnalyzers = default)
        {
            var forAnalyzersInstance = _jvmObjectReference.Jvm.CallStaticJavaMethod(
                "com.amazon.deequ.analyzers.runners.AnalyzerContext", "successMetricsAsDataFrame$default$3");

            var jvmReference = (IJvmObjectReferenceProvider)SparkSession.GetActiveSession();

            var dataFrameReference = (JvmObjectReference)_jvmObjectReference.Jvm.CallStaticJavaMethod(
                "com.amazon.deequ.analyzers.runners.AnalyzerContext",
                "successMetricsAsDataFrame",
                jvmReference.Reference, _jvmObjectReference, forAnalyzersInstance);
            return new DataFrame(dataFrameReference);
        }

        public MapJvm MetricMap()
        {
            var metricMap = (JvmObjectReference)
                _jvmObjectReference.Jvm.CallNonStaticJavaMethod(_jvmObjectReference, "metricMap");

            return new MapJvm(metricMap);
        }


        public string SuccessMetricsAsJson(IEnumerable<IAnalyzer<IMetric>> forAnalyzers = default)
        {
            var forAnalyzersInstance = _jvmObjectReference.Jvm.CallStaticJavaMethod(
                "com.amazon.deequ.analyzers.runners.AnalyzerContext", "successMetricsAsJson$default$2");


            return (string) _jvmObjectReference.Jvm.CallStaticJavaMethod(
                "com.amazon.deequ.analyzers.runners.AnalyzerContext", "successMetricsAsJson", _jvmObjectReference, forAnalyzersInstance);
        }


    }
}
