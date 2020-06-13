using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using static xdeequ.Analyzers.Initializers;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class SizeAnalyzer
    {
        private readonly SparkSession _session;

        public SizeAnalyzer(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        [Fact]
        public void compute_correct_metrics_missing()
        {
            DataFrame missing = FixtureSupport.GetDFMissing(_session);

            DoubleMetric actualMissing = Size(new Option<string>()).Calculate(missing);
            DoubleMetric expectedMissing = DoubleMetric.Create(Entity.DataSet, "Size", "*", missing.Count());

            actualMissing.Entity.ShouldBe(expectedMissing.Entity);
            actualMissing.Name.ShouldBe(expectedMissing.Name);
            actualMissing.Instance.ShouldBe(expectedMissing.Instance);
            actualMissing.Value.Get().ShouldBe(expectedMissing.Value.Get());
        }

        [Fact]
        public void compute_correct_metrics_full()
        {
            DataFrame full = FixtureSupport.GetDFFull(_session);

            DoubleMetric actualFull = Size(new Option<string>()).Calculate(full);
            DoubleMetric expectedFull = DoubleMetric.Create(Entity.DataSet, "Size", "*", full.Count());

            actualFull.Entity.ShouldBe(expectedFull.Entity);
            actualFull.Name.ShouldBe(expectedFull.Name);
            actualFull.Instance.ShouldBe(expectedFull.Instance);
            actualFull.Value.Get().ShouldBe(expectedFull.Value.Get());
        }
    }
}