using Microsoft.Spark.Interop.Ipc;

namespace deequ.Interop
{
    public class ExceptionJvm : IJvmObjectReferenceProvider
    {
        public JvmObjectReference Reference { get; }

        private JvmObjectReference _jvmObject;

        public ExceptionJvm(JvmObjectReference reference)
        {
            _jvmObject = reference;
        }
        public override string ToString() => (string) _jvmObject.Invoke("toString");
        /// <summary>
        ///
        /// </summary>
        /// <param name="jvmObjectReference"></param>
        /// <returns></returns>
        public static implicit operator ExceptionJvm(JvmObjectReference jvmObjectReference)
        {
            return new ExceptionJvm(jvmObjectReference);
        }

        public string GetMessage() => (string)_jvmObject.Invoke("getMessage");
    }
}
