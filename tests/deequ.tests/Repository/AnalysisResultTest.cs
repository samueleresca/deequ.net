using Microsoft.Spark.Sql;
using Xunit;

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class AnalysisResultTest
    {
        private readonly SparkSession _session;

        public AnalysisResultTest(SparkFixture fixture) => _session = fixture.Spark;
    }
}
