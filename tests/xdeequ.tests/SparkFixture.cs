// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Spark.Sql;
using Xunit;

namespace xdeequ.tests
{
    /// <summary>
    ///     SparkFixture acts as a global fixture to start Spark application in a debug
    ///     mode through the spark-submit. It also provides a default SparkSession
    ///     object that any tests can use.
    /// </summary>
    public sealed class SparkFixture : IDisposable
    {
        public const string DefaultLogLevel = "ERROR";

        private readonly Process _process = new Process();
        private readonly TemporaryDirectory _tempDirectory = new TemporaryDirectory();


        public SparkFixture()
        {
            // The worker directory must be set for the Microsoft.Spark.Worker executable.
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(EnvironmentVariableNames.WorkerDir)))
                throw new Exception(
                    $"Environment variable '{EnvironmentVariableNames.WorkerDir}' must be set.");

            BuildSparkCmd(out var filename, out var args);

            // Configure the process using the StartInfo properties.
            _process.StartInfo.FileName = filename;
            _process.StartInfo.Arguments = args;
            // UseShellExecute defaults to true in .NET Framework,
            // but defaults to false in .NET Core. To support both, set it
            // to false which is required for stream redirection.
            _process.StartInfo.UseShellExecute = false;
            _process.StartInfo.RedirectStandardInput = true;
            _process.StartInfo.RedirectStandardOutput = true;
            _process.StartInfo.RedirectStandardError = true;

            var isSparkReady = false;
            _process.OutputDataReceived += (sender, arguments) =>
            {
                // Scala-side driver for .NET emits the following message after it is
                // launched and ready to accept connections.
                if (!isSparkReady &&
                    arguments.Data.Contains("Backend running debug mode"))
                    isSparkReady = true;
            };

            _process.Start();
            _process.BeginOutputReadLine();

            var processExited = false;
            while (!isSparkReady && !processExited) processExited = _process.WaitForExit(500);

            if (processExited)
            {
                _process.Dispose();

                // The process should not have been exited.
                throw new Exception(
                    $"Process exited prematurely with '{filename} {args}'.");
            }

            Spark = SparkSession
                .Builder()
                // Lower the shuffle partitions to speed up groupBy() operations.
                .Config("spark.sql.shuffle.partitions", "3")
                .Config("spark.ui.enabled", true)
                .Config("spark.ui.showConsoleProgress", true)
                .AppName("Microsoft.Spark.E2ETest")
                .GetOrCreate();

            Spark.SparkContext.SetLogLevel(DefaultLogLevel);
        }

        internal SparkSession Spark { get; }

        public void Dispose()
        {
            Spark.Dispose();

            // CSparkRunner will exit upon receiving newline from
            // the standard input stream.
            _process.StandardInput.WriteLine("done");
            _process.StandardInput.Flush();
            _process.WaitForExit();

            _tempDirectory.Dispose();
        }

        private void BuildSparkCmd(out string filename, out string args)
        {
            var sparkHome = SparkSettings.SparkHome;

            // Build the executable name.
            filename = Path.Combine(sparkHome, "bin", "spark-submit");
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) filename += ".cmd";

            if (!File.Exists(filename)) throw new FileNotFoundException($"{filename} does not exist.");

            // Build the arguments for the spark-submit.
            var classArg = "--class org.apache.spark.deploy.dotnet.DotnetRunner";
            var curDir = AppDomain.CurrentDomain.BaseDirectory;
            var jarPrefix = GetJarPrefix();
            var scalaDir = Path.Combine(curDir, "..", "..", "..", "scala");
            var jarDir = Path.Combine(scalaDir, jarPrefix, "target");
            var assemblyVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString(3);
            var jar = Path.Combine(jarDir, $"{jarPrefix}-{assemblyVersion}.jar");

            if (!File.Exists(jar)) throw new FileNotFoundException($"{jar} does not exist.");

            var warehouseUri = new Uri(
                Path.Combine(_tempDirectory.Path, "spark-warehouse")).AbsoluteUri;
            var warehouseDir = $"--conf spark.sql.warehouse.dir={warehouseUri}";

            var extraArgs = Environment.GetEnvironmentVariable(
                EnvironmentVariableNames.ExtraSparkSubmitArgs) ?? "";

            // If there exists log4j.properties in SPARK_HOME/conf directory, Spark from 2.3.*
            // to 2.4.0 hang in E2E test. The reverse behavior is true for Spark 2.4.1; if
            // there does not exist log4j.properties, the tests hang.
            // Note that the hang happens in JVM when it tries to append a console logger (log4j).
            // The solution is to use custom log configuration that appends NullLogger, which
            // works across all Spark versions.
            var resourceUri = new Uri(TestEnvironment.ResourceDirectory).AbsoluteUri;
            var logOption = "--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=" +
                            $"{resourceUri}/log4j.properties";

            args = $"{logOption} {warehouseDir} {extraArgs} {classArg} --master local {jar} debug";
        }

        private string GetJarPrefix()
        {
            var sparkVersion = SparkSettings.Version;
            return $"microsoft-spark-{sparkVersion.Major}.{sparkVersion.Minor}.x";
        }

        /// <summary>
        ///     The names of environment variables used by the SparkFixture.
        /// </summary>
        public class EnvironmentVariableNames
        {
            /// <summary>
            ///     This environment variable specifies extra args passed to spark-submit.
            /// </summary>
            public const string ExtraSparkSubmitArgs =
                "DOTNET_SPARKFIXTURE_EXTRA_SPARK_SUBMIT_ARGS";

            /// <summary>
            ///     This environment variable specifies the path where the DotNet worker is installed.
            /// </summary>
            public const string WorkerDir = "DOTNET_WORKER_DIR";
        }
    }

    [CollectionDefinition("Spark instance")]
    public class SparkCollection : ICollectionFixture<SparkFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}