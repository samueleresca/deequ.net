using Microsoft.Spark.Interop.Ipc;

namespace deequ.Util
{

    internal sealed class Seq<T> : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Seq(JvmObjectReference jvmObject, params T[] values)
        {
            _jvmObject = jvmObject.Jvm.CallConstructor("scala.collection.immutable.Seq", values);
        }

        public JvmObjectReference Reference => _jvmObject;

    }
}
