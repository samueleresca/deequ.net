using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Interop.Utils
{

    internal sealed class SeqJvm : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal SeqJvm( params object[] values)
        {
            var array = new ArrayList(SparkEnvironment.JvmBridge);

            array.AddAll(values);

            var iterator = (IJvmObjectReferenceProvider)SparkEnvironment.JvmBridge.CallStaticJavaMethod("scala.collection.JavaConversions",
                "asScalaIterator", ((IJvmObjectReferenceProvider)array).Reference.Invoke("iterator"));
            _jvmObject = (JvmObjectReference)iterator.Reference.Invoke("toList");
        }

        public JvmObjectReference Reference => _jvmObject;

    }
}
