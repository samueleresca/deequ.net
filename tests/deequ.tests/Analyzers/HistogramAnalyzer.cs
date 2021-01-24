using System;
using System.Collections.Generic;
using deequ.Metrics;
using deequ.tests.Analyzers;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static Microsoft.Spark.Sql.Functions;
using static deequ.Analyzers.Initializers;
using Histogram = deequ.Analyzers.Histogram;


namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class HistogramAnalyzer
    {
        public HistogramAnalyzer(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics_after_binning_if_provided()
        {
            Func<Column, Column> custom = Udf<string, string>(column =>
            {
                return column switch
                {
                    "a" => "Value1",
                    "b" => "Value1",
                    _ => "Value2"
                };
            });

            DataFrame complete = FixtureSupport.GetDFMissing(_session);
            HistogramMetric histogram = Histogram("att1", custom).Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();

            histogram.Value.Get().NumberOfBins.ShouldBe(2);
            histogram.Value.Get().Values.Keys.ShouldContain("Value1");
            histogram.Value.Get().Values.Keys.ShouldContain("Value2");
        }

        [Fact]
        public void compute_correct_metrics_missing()
        {
            DataFrame complete = FixtureSupport.GetDFMissing(_session);
            HistogramMetric histogram = Histogram("att1").Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();
            histogram.Value.Get().Values.Count.ShouldBe(3);

            Dictionary<string, DistributionValue>.KeyCollection keys = histogram
                .Value
                .Get()
                .Values
                .Keys;

            keys.ShouldContain("a");
            keys.ShouldContain("b");
            keys.ShouldContain(Histogram.NULL_FIELD_REPLACEMENT);
        }

        [Fact]
        public void compute_correct_metrics_on_numeric_values()
        {
            DataFrame complete = FixtureSupport.GetDfWithNumericValues(_session);
            HistogramMetric histogram = Histogram("att2").Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();

            histogram.Value.Get().NumberOfBins.ShouldBe(4);
            histogram.Value.Get().Values.Count.ShouldBe(4);
        }

        [Fact]
        public void compute_correct_metrics_should_only_get_top_N_bins()
        {
            DataFrame complete = FixtureSupport.GetDFMissing(_session);
            HistogramMetric histogram = Histogram("att1", new Option<string>(), 2).Calculate(complete);

            histogram.Value.IsSuccess.ShouldBeTrue();

            histogram.Value.Get().NumberOfBins.ShouldBe(3);
            histogram.Value.Get().Values.Count.ShouldBe(2);
            histogram.Value.Get().Values.Keys.ShouldContain("a");
            histogram.Value.Get().Values.Keys.ShouldContain(Histogram.NULL_FIELD_REPLACEMENT);
        }

        [Fact]
        public void fail_for_max_detail_bins_greater_than_1000()
        {
            DataFrame complete = FixtureSupport.GetDFFull(_session);
            HistogramMetric histogram = Histogram("att1", new Option<string>(), 1002).Calculate(complete);
            histogram.Value.IsSuccess.ShouldBeFalse();
        }
    }
}
