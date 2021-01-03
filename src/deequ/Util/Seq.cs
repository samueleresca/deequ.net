using deequ.Analyzers;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Util
{

    internal sealed class Seq : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Seq( params object[] values)
        {

            var array = new ArrayList(SparkEnvironment.JvmBridge);
            array.AddAll(values);
            var iterator = (IJvmObjectReferenceProvider)SparkEnvironment.JvmBridge.CallStaticJavaMethod("scala.collection.JavaConversions",
                "asScalaIterator", ((IJvmObjectReferenceProvider)array).Reference.Invoke("iterator"));
            _jvmObject = (JvmObjectReference)iterator.Reference.Invoke("toSeq");
        }

        public JvmObjectReference Reference => _jvmObject;

    }
}
