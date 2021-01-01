using System;
using System.IO;
using deequ.Analyzers;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Xunit;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class BasicStatistics
    {
        public BasicStatistics(SparkFixture fixture) => _fixture = fixture;

        private  SparkSession _session => _fixture.Spark;
        private readonly SparkFixture _fixture;

        [Fact]
        public void compute_maximum_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            AnalysisRunBuilder builder = new AnalysisRunBuilder(df, SparkEnvironment.JvmBridge);

            Maximum result = new Maximum("att1");

            AnalyzerContext context = builder
                .AddAnalyzer(result)
                .Run();

            string resultJson = context
                .SuccessMetricsAsJson();

            var test2 = context.MetricMap();

            DataFrame resultFrame =  context
                .SuccessMetricsAsDataFrame();

        }

    }

    /// <summary>
    /// <see cref="DataStreamWriter.ForeachBatch(Action{DataFrame, long})"/> callback handler.
    /// </summary>
    internal sealed class ForeachBatchCallbackHandler : ICallbackHandler
    {
        private readonly Func<long, bool> _func;

        internal ForeachBatchCallbackHandler(Func<long, bool> func) =>
            _func = func;


        public void Run(Stream inputStream)
        {
            long test = SerDe.ReadInt64(inputStream);
            _func(test);
        }
    }
}
