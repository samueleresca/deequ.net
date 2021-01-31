using System;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Interop.Utils
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
            if (typeof(T).IsValueType || typeof(T) == typeof(String))
            {
                return  _jvmObject.Invoke("get");
            }

            return (JvmObjectReference)_jvmObject.Invoke("get");
        }

        public T Get<T>() where T: struct
        {
            if (typeof(T).IsValueType || typeof(T) == typeof(String))
            {
                return (T) _jvmObject.Invoke("get");
            }

            throw new Exception();
        }
    }
}
