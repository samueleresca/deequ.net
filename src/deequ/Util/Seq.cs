using deequ.Analyzers;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Util
{

    internal sealed class Seq<T> : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Seq(JvmObjectReference jvmObject, params object[] values)
        {

            var array = new ArrayList(jvmObject.Jvm);
            array.AddAll(values);
            var iterator = (IJvmObjectReferenceProvider)SparkEnvironment.JvmBridge.CallStaticJavaMethod("scala.collection.JavaConversions",
                "asScalaIterator", ((IJvmObjectReferenceProvider)array).Reference.Invoke("iterator"));
            _jvmObject = (JvmObjectReference)iterator.Reference.Invoke("toSeq");
        }

        public JvmObjectReference Reference => _jvmObject;

    }
}
