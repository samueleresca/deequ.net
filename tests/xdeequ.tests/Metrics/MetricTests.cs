using System;
using System.Collections.Generic;
using System.Linq;
using Shouldly;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Metrics
{
    public class MetricTests
    {
        public static ArgumentException sampleException = new ArgumentException("");

        [Fact]
        public void DoubleMetric_should_flatten_and_return_itself()
        {
            DoubleMetric metric =
                new DoubleMetric(Entity.Column, "metric-name", "instance-name", Try<double>.From(() => 50));
            metric.Flatten().ShouldBe(new List<DoubleMetric> {metric});
        }

        [Fact]
        public void DoubleMetric_should_flatten_in_case_of_error()
        {
            DoubleMetric metric = new DoubleMetric(Entity.Column, "metric-name", "instance-name",
                Try<double>.From(() => throw sampleException));
            metric.Flatten().ShouldBe(new List<DoubleMetric> {metric});
        }

        [Fact]
        public void DoubleMetric_should_flatten_matched_and_unmatched_in_case_of_an_error()
        {
            HistogramMetric metric =
                new HistogramMetric("instance-name", Try<Distribution>.From(() => throw sampleException));
            List<DoubleMetric> expected = new List<DoubleMetric>
            {
                new DoubleMetric(Entity.Column, "Histogram.bins", "instance-name",
                    Try<double>.From(() => throw sampleException))
            };
            metric.Flatten().Any(x => x.Name == "Histogram.bins").ShouldBeTrue();
            metric.Flatten().Any(x => x.Name == "Histogram.abs.a").ShouldBeFalse();
            metric.Flatten().Any(x => x.Name == "Histogram.abs.b").ShouldBeFalse();
            metric.Flatten().Any(x => x.Name == "Histogram.ratio.a").ShouldBeFalse();
            metric.Flatten().Any(x => x.Name == "Histogram.ratio.b").ShouldBeFalse();
        }

        [Fact]
        public void HistogramMetric_should_flatten_and_return_itself()
        {
            Distribution distribution =
                new Distribution(
                    new Dictionary<string, DistributionValue>
                    {
                        {"a", new DistributionValue(6, .6)}, {"b", new DistributionValue(4, .4)}
                    }, 2);

            HistogramMetric metric = new HistogramMetric("instance-name", Try<Distribution>.From(() => distribution));

            List<DoubleMetric> expected = new List<DoubleMetric>
            {
                new DoubleMetric(Entity.Column, "Histogram.bins", "instance-name", Try<double>.From(() => 2)),
                new DoubleMetric(Entity.Column, "Histogram.abs.a", "instance-name", Try<double>.From(() => 6)),
                new DoubleMetric(Entity.Column, "Histogram.abs.b", "instance-name", Try<double>.From(() => 4)),
                new DoubleMetric(Entity.Column, "Histogram.ratio.a", "instance-name", Try<double>.From(() => .6)),
                new DoubleMetric(Entity.Column, "Histogram.ratio.b", "instance-name", Try<double>.From(() => .4))
            };

            metric.Flatten().Any(x => x.Name == "Histogram.bins").ShouldBeTrue();
            metric.Flatten().Any(x => x.Name == "Histogram.abs.a").ShouldBeTrue();
            metric.Flatten().Any(x => x.Name == "Histogram.abs.b").ShouldBeTrue();
            metric.Flatten().Any(x => x.Name == "Histogram.ratio.a").ShouldBeTrue();
            metric.Flatten().Any(x => x.Name == "Histogram.ratio.b").ShouldBeTrue();
        }
    }
}
