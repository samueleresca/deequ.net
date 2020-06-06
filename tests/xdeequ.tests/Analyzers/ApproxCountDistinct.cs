using Microsoft.Spark.Sql;
using Xunit;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class ApproxCountDistinct
    {
        private readonly SparkSession _session;

        public ApproxCountDistinct(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        [Fact(Skip = "Not implemented")]
        public void compute_approximate_distinct_count_for_numeric_data()
        {
        }
    }
}