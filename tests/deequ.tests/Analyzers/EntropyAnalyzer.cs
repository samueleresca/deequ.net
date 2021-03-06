using System;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static deequ.Analyzers.Initializers;

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


            DoubleMetric expected1 = DoubleMetric.Create(MetricEntity.Column, "Entropy", "att1", new Try<double>(0));
            DoubleMetric expected2 = DoubleMetric.Create(MetricEntity.Column, "Entropy", "att2",
                -(0.75 * Math.Log(0.75) + 0.25 * Math.Log(0.25)));


            attr1.MetricEntity.ShouldBe(expected1.MetricEntity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.IsSuccess.ShouldBeTrue();

            attr2.MetricEntity.ShouldBe(expected2.MetricEntity);
            attr2.Instance.ShouldBe(expected2.Instance);
            attr2.Name.ShouldBe(expected2.Name);
            attr2.Value.Get().ShouldBe(expected2.Value.Get());
        }

        [Fact]
        public void entropy_correctly_tostring_instances()
        {
            Entropy("att1").ToString().ShouldBe("Entropy(att1,None)");
        }
    }
}
