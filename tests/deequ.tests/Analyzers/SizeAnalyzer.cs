using deequ.Interop;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static deequ.Analyzers.Initializers;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class SizeAnalyzer
    {
        public SizeAnalyzer(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics_full()
        {
            DataFrame full = FixtureSupport.GetDFFull(_session);

            DoubleMetricJvm actualFull = Size(Option<string>.None).Calculate(full);
            DoubleMetric expectedFull = DoubleMetric.Create(MetricEntity.Dataset, "Size", "*", full.Count());

            actualFull.Name.ShouldBe(expectedFull.Name);
            actualFull.Instance.ShouldBe(expectedFull.Instance);
            actualFull.Value.Get().ShouldBe(expectedFull.Value.Get());
        }

        [Fact]
        public void compute_correct_metrics_missing()
        {
            DataFrame missing = FixtureSupport.GetDFMissing(_session);

            DoubleMetricJvm actualMissing = Size(Option<string>.None).Calculate(missing);
            DoubleMetric expectedMissing = DoubleMetric.Create(MetricEntity.Dataset, "Size", "*", missing.Count());

            actualMissing.Name.ShouldBe(expectedMissing.Name);
            actualMissing.Instance.ShouldBe(expectedMissing.Instance);
            actualMissing.Value.Get().ShouldBe(expectedMissing.Value.Get());
        }

        [Fact]
        public void size_correctly_tostring_instances()
        {
            Size(Option<string>.None).ToString().ShouldBe("Size(None)");
        }
    }
}
