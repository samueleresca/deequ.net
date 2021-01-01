using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Util
{
    /// <summary>
    /// Hashtable class represents a <c>java.util.Hashtable</c> object.
    /// </summary>
    public sealed class Map : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        /// <summary>
        /// Create a <c>java.util.Hashtable</c> JVM object
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        internal Map(IJvmBridge jvm) =>
            _jvmObject = jvm.CallConstructor("scala.collection.immutable.Map");

        public JvmObjectReference Reference => _jvmObject;

        /// <summary>
        /// Maps the specified key to the specified value in this Map.
        /// Neither the key nor the value can be null.
        /// </summary>
        /// <param name="key">The Map key</param>
        /// <param name="value">The value</param>
        internal void Put(object key, object value) =>
            _jvmObject.Invoke("put", key, value);

        /// <summary>
        /// Associates the specified value with the specified key in this map (optional operation). If the map previously
        /// contained a mapping for the key, the old value is replaced by the specified value.
        /// </summary>
        /// <param name="key">The key</param>
        internal void Get(object key) =>
            _jvmObject.Invoke("get", key);


        internal void PutAll<K, V>(Dictionary<K, V> dict)
        {
            foreach (KeyValuePair<K, V> pair in dict)
                Put(pair.Key, pair.Value);
        }
    }
}
