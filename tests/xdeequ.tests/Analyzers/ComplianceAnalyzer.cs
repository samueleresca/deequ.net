using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using static xdeequ.Analyzers.Initializers;
using static Microsoft.Spark.Sql.Functions;


namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class ComplianceAnalyzer
    {
        public ComplianceAnalyzer(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics()
        {
            var df = FixtureSupport.GetDfWithNumericValues(_session);

            var attr1 = Compliance("rule1", Expr("att1 > 3")).Calculate(df);
            var attr2 = Compliance("rule2", Expr("att1 > 2")).Calculate(df);

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
            var df = FixtureSupport.GetDfWithNumericValues(_session);

            var attr1 = Compliance("rule1", Expr("att2 = 0"),
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
            var df = FixtureSupport.GetDfWithNumericValues(_session);

            var attr1 = Compliance("rule1", Expr("attNoSuchColumn > 3")).Calculate(df);

            var expected1 = DoubleMetric.Create(Entity.Column, "Compliance", "rule1", 1.0);

            attr1.Entity.ShouldBe(expected1.Entity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.IsSuccess.ShouldBeFalse();
        }
    }
}