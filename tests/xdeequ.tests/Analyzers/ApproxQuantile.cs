using Microsoft.Spark.Sql;
using Xunit;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class ApproxQuantile
    {
        private readonly SparkSession _session;

        public ApproxQuantile(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        [Fact(Skip = "Not implemented")]
        public void approximate_quantile_0_5_within_acceptable_error_bound()
        {
        }
    }
}