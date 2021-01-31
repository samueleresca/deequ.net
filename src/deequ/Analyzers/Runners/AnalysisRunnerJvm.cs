using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers.Runners
{
    public class AnalysisRunnerJvm
    {

        private static string classNamespace => "com.amazon.deequ.analyzers.runners.AnalysisRunner";
        private JvmObjectReference _jvmObjectReference;

        public static AnalysisRunBuilderJvm OnData(DataFrame data)
        {
            JvmObjectReference dataFrameRef = ((IJvmObjectReferenceProvider)data).Reference;

            JvmObjectReference reference = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallStaticJavaMethod(classNamespace,
                "onData", dataFrameRef);

            return new AnalysisRunBuilderJvm(reference);
        }


        public static AnalyzerContext Run(DataFrame data, AnalysisJvm analyzers,
            Option<IStateLoader> aggregateWith = default,
            Option<IStatePersister> saveStatesWith = default,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK)
        {
            JvmObjectReference dataFrameRef = ((IJvmObjectReferenceProvider)data).Reference;

            var aggregateWithDef = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallStaticJavaMethod(classNamespace,
                    "run$default$3");

            var saveStatesWithDef = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallStaticJavaMethod(classNamespace,
                    "run$default$4");

            var storageLevelOfGroupedDataForMultiplePassesDef = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallStaticJavaMethod(classNamespace,
                    "run$default$5");

            JvmObjectReference reference = (JvmObjectReference)SparkEnvironment.JvmBridge
                .CallStaticJavaMethod(classNamespace,
                    "run", dataFrameRef, analyzers.Reference, aggregateWithDef,
                    saveStatesWithDef, storageLevelOfGroupedDataForMultiplePassesDef);

            return  new AnalyzerContext(reference);
        }


    }

    public class AnalysisRunBuilderJvm
    {
        private JvmObjectReference _reference;

        public AnalysisRunBuilderJvm(JvmObjectReference reference)
        {
            _reference = reference;
        }
    }
}
