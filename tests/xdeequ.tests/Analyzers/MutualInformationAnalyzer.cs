using System;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Metrics;
using Xunit;
using static xdeequ.Analyzers.Initializers;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class MutualInformationAnalyzer
    {
        public MutualInformationAnalyzer(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics_missing()
        {
            var complete = FixtureSupport.GetDFFull(_session);

            var attr1 = MutualInformation(new[] { "att1", "att2" }).Calculate(complete);
            var expected1 = DoubleMetric
                .Create(Entity.MultiColumn, "MutualInformation", "att1,att2",
                    -(0.75 * Math.Log(0.75) + 0.25 * Math.Log(0.25)));

            attr1.Entity.ShouldBe(expected1.Entity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.Get().ShouldBe(expected1.Value.Get());
        }

        [Fact]
        public void compute_entropy_for_same_column()
        {
            var complete = FixtureSupport.GetDFFull(_session);

            var entropyViaMI = MutualInformation(new[] { "att1", "att2" }).Calculate(complete);
            var entropy = Entropy("att1").Calculate(complete);

            entropyViaMI.Value.IsSuccess.ShouldBeTrue();
            entropy.Value.IsSuccess.ShouldBeTrue();
            entropyViaMI.Value.Get().ShouldBe(entropy.Value.Get());
        }

        [Fact]
        public void yields_0_for_conditionally_uninformative_columns()
        {
            var complete = FixtureSupport.GetDfWithConditionallyUninformativeColumns(_session);
            MutualInformation(new[] { "att1", "att2" }).Calculate(complete).Value.Get().ShouldBe(0);
        }
    }
}