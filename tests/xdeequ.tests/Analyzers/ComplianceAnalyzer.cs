using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using static xdeequ.Analyzers.Inizializers;


namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class ComplianceAnalyzer
    {
        private readonly SparkSession _session;

        public ComplianceAnalyzer(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        [Fact]
        public void compute_correct_metrics()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            var attr1 = Compliance("rule1", "att1 > 3").Calculate(df);
            var attr2 = Compliance("rule2", "att1 > 2").Calculate(df);

            var expected1 = DoubleMetric.Create(Entity.Column, "Compliance", "rule1", 3.0 / 6);
            var expected2 = DoubleMetric.Create(Entity.Column, "Compliance", "rule2", 4.0 / 6);

            attr1.Entity.ShouldBe(expected1.Entity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.Get().ShouldBe(expected1.Value.Get());

            attr2.Entity.ShouldBe(expected2.Entity);
            attr2.Instance.ShouldBe(expected2.Instance);
            attr2.Name.ShouldBe(expected2.Name);
            attr2.Value.Get().ShouldBe(expected2.Value.Get());
        }

        [Fact]
        public void compute_correct_metrics_with_filtering()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            var attr1 = Compliance("rule1", "att2 = 0",
                new Option<string>("att1 < 4")).Calculate(df);

            var expected1 = DoubleMetric.Create(Entity.Column, "Compliance", "rule1", 1.0);

            attr1.Entity.ShouldBe(expected1.Entity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.Get().ShouldBe(expected1.Value.Get());
        }

        [Fact]
        public void fail_on_wrong_column_input()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            var attr1 = Compliance("rule1", "attNoSuchColumn > 3").Calculate(df);

            var expected1 = DoubleMetric.Create(Entity.Column, "Compliance", "rule1", 1.0);

            attr1.Entity.ShouldBe(expected1.Entity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.IsSuccess.ShouldBeFalse();
        }
    }
}