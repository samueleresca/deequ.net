using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Internal.Scala;
using Microsoft.Spark.Interop.Ipc;

namespace deequ.Interop.Utils
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

        internal Map(JvmObjectReference jvm) =>
            _jvmObject = jvm;

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
        internal OptionJvm Get(object key)
        {
            var valueReference= (JvmObjectReference) _jvmObject.Invoke("get", key);
            return new OptionJvm(valueReference);
        }


        public JvmObjectReference First()
        {
            JvmObjectReference valueReference = (JvmObjectReference) _jvmObject.Invoke("head");
            return valueReference;
        }

        internal List<V> GetValues<V>()
        {
            List<V> result = new List<V>();
            Seq<object> sequence = new Seq<object>((JvmObjectReference)((JvmObjectReference)
                _jvmObject.Invoke("values")).Invoke("toSeq"));


            foreach (object value in sequence)
            {
                if (typeof(V).IsValueType || typeof(V) == typeof(String))
                {
                    result.Add((V) value);
                }
                else
                {
                    result.Add((V)Activator.CreateInstance(typeof(V), (JvmObjectReference)value));
                }

            }

            return result;
        }


        internal void PutAll<K, V>(Dictionary<K, V> dict)
        {
            foreach (KeyValuePair<K, V> pair in dict)
                Put(pair.Key, pair.Value);
        }
    }
}
