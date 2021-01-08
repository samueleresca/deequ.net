using System;
using Microsoft.Spark.Interop.Internal.Scala;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Util
{
    public class TryJvm<T>
    {

        private readonly JvmObjectReference _jvmObject;

        internal TryJvm(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        public bool IsSuccess() => (bool) _jvmObject.Invoke("isSuccess");

        public object Get()
        {
            return _jvmObject.Invoke("get");
        }
    }
}
