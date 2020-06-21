using System;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using static xdeequ.Analyzers.Initializers;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class EntropyAnalyzer
    {
        public EntropyAnalyzer(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics_missing()
        {
            DataFrame complete = FixtureSupport.GetDFFull(_session);

            DoubleMetric attr1 = Entropy("att1").Calculate(complete);
            DoubleMetric attr2 = Entropy("att2").Calculate(complete);


            DoubleMetric expected1 = DoubleMetric.Create(Entity.Column, "Entropy", "att1", new Try<double>(0));
            DoubleMetric expected2 = DoubleMetric.Create(Entity.Column, "Entropy", "att2",
                -(0.75 * Math.Log(0.75) + 0.25 * Math.Log(0.25)));


            attr1.Entity.ShouldBe(expected1.Entity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.IsSuccess.ShouldBeTrue();

            attr2.Entity.ShouldBe(expected2.Entity);
            attr2.Instance.ShouldBe(expected2.Instance);
            attr2.Name.ShouldBe(expected2.Name);
            attr2.Value.Get().ShouldBe(expected2.Value.Get());
        }
    }
}
