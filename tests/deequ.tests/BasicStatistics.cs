using System.Collections.Generic;
using deequ.Analyzers;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
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

            MaximumJvm result = new MaximumJvm("att1");

            AnalzyerContext context = builder
                .AddAnalyzer(result)
                .Run();

            string resultJson = context
                .SuccessMetricsAsJson();

            DataFrame resultFrame =  context
                .SuccessMetricsAsDataFrame();
                
        }

    }
}
