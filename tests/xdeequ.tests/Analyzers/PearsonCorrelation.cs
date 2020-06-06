using Microsoft.Spark.Sql;
using Xunit;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class PearsonCorrelation
    {
        private readonly SparkSession _session;

        public PearsonCorrelation(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        [Fact(Skip = "Not implemented")]
        public void yield_NaN_for_conditionally_uninformative_columns()
        {
        }
    }
}