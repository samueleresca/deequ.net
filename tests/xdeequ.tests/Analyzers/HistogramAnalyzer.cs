using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Util;
using Xunit;
using static Microsoft.Spark.Sql.Functions;
using static xdeequ.Analyzers.Initializers;
using Histogram = xdeequ.Analyzers.Histogram;


namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class HistogramAnalyzer
    {
        public HistogramAnalyzer(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics_after_binning_if_provided()
        {
            var custom = Udf<string, string>(column =>
            {
                return column switch
                {
                    "a" => "Value1",
                    "b" => "Value1",
                    _ => "Value2"
                };
            });

            var complete = FixtureSupport.GetDFMissing(_session);
            var histogram = Histogram("att1", custom).Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();

            histogram.Value.Get().NumberOfBins.ShouldBe(2);
            histogram.Value.Get().Values.Keys.ShouldContain("Value1");
            histogram.Value.Get().Values.Keys.ShouldContain("Value2");
        }

        [Fact]
        public void compute_correct_metrics_missing()
        {
            var complete = FixtureSupport.GetDFMissing(_session);
            var histogram = Histogram("att1").Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();
            histogram.Value.Get().Values.Count.ShouldBe(3);

            var keys = histogram
                .Value
                .Get()
                .Values
                .Keys;

            keys.ShouldContain("a");
            keys.ShouldContain("b");
            keys.ShouldContain(Histogram.NullFieldReplacement);
        }

        [Fact]
        public void compute_correct_metrics_on_numeric_values()
        {
            var complete = FixtureSupport.GetDfWithNumericValues(_session);
            var histogram = Histogram("att2").Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();

            histogram.Value.Get().NumberOfBins.ShouldBe(4);
            histogram.Value.Get().Values.Count.ShouldBe(4);
        }

        [Fact]
        public void compute_correct_metrics_should_only_get_top_N_bins()
        {
            var complete = FixtureSupport.GetDFMissing(_session);
            var histogram = Histogram("att1", new Option<string>(), 2).Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();

            histogram.Value.Get().NumberOfBins.ShouldBe(3);
            histogram.Value.Get().Values.Count.ShouldBe(2);
            histogram.Value.Get().Values.Keys.ShouldContain("a");
            histogram.Value.Get().Values.Keys.ShouldContain(Histogram.NullFieldReplacement);
        }

        [Fact]
        public void fail_for_max_detail_bins_greater_than_1000()
        {
            var complete = FixtureSupport.GetDFFull(_session);
            var histogram = Histogram("att1", new Option<string>(), 1002).Calculate(complete);
            histogram.Value.IsSuccess.ShouldBeFalse();
        }
    }
}