using System.Linq;
using deequ.Metrics;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static deequ.Analyzers.Initializers;
namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class CorrelationAnalyzer
    {
        private readonly SparkSession _session;

        public CorrelationAnalyzer(SparkFixture fixture) => _session = fixture.Spark;


        [Fact]
        public void compute_correct_metrics_missing()
        {
            DataFrame numericValues = FixtureSupport.GetDfWithNumericValues(_session);

            Correlation("att1", "att2").Preconditions().Count().ShouldBe(4);

            DoubleMetric attCorrelation = Correlation("att1", "att2").Calculate(numericValues);

            DoubleMetric expected1 = DoubleMetric.Create(Entity.Multicolumn, "Correlation", "att1,att2", 0.5);

            attCorrelation.Entity.ShouldBe(expected1.Entity);
            attCorrelation.Instance.ShouldBe(expected1.Instance);
            attCorrelation.Name.ShouldBe(expected1.Name);
            attCorrelation.Value.Get().ShouldBe(expected1.Value.Get());
        }

    }
}
