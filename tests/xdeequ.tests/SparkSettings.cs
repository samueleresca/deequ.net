// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using Xunit.Sdk;

namespace xdeequ.tests
{
    internal static class SparkSettings
    {
        static SparkSettings()
        {
            InitSparkHome();
            InitVersion();
        }

        internal static Version Version { get; private set; }
        internal static string SparkHome { get; private set; }

        private static void InitSparkHome()
        {
            SparkHome =
                $"{Environment.GetEnvironmentVariable("HOME")}{Path.DirectorySeparatorChar}bin/spark-2.4.4-bin-hadoop2.7";
            if (SparkHome == null) throw new NullException("SPARK_HOME environment variable is not set.");
        }

        private static void InitVersion()
        {
            // First line of the RELEASE file under SPARK_HOME will be something similar to:
            // Spark 2.3.2 built for Hadoop 2.7.3
            var firstLine =
                File.ReadLines($"{SparkHome}{Path.DirectorySeparatorChar}RELEASE").First();
            Version = new Version(firstLine.Split(' ')[1]);
        }
    }
}