using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Shouldly;
using xdeequ.AnomalyDetection;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.AnomalyDetection
{
    public class AbsoluteChangeStrategyTests
    {

        private AbsoluteChangeStrategy _strategy = new AbsoluteChangeStrategy(-2.0, 2.0);

        private double[] data = Enumerable.Range(0, 50).Select(x =>
        {
            if (x < 20 || x > 30)
                return 1.0;

            if (x % 2 == 0)
                return x;

            return -x;

        }).ToArray();



        [Fact]
        public void AbsoluteChangeStrategy_detect_all_anomalies_if_no_interval_specified()
        {
            var anomalyResult = _strategy.Detect(data.ToArray(), (0, int.MaxValue));
            var expected = Enumerable.Range(20, 12)
                .Select(x => (x, new Anomaly(data[x], 1.0, Option<string>.None)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void AbsoluteChangeStrategy_only_detect_anomalies_in_interval()
        {
            var anomalyResult = _strategy.Detect(data.ToArray(), (25, 50));
            var expected = Enumerable.Range(25, 7)
                .Select(x => (x, new Anomaly(data[x], 1.0, Option<string>.None)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void AbsoluteChangeStrategy_ignore_min_rate_if_none_is_given()
        {
            var strategy = new AbsoluteChangeStrategy(Option<double>.None, 1.0);

            var anomalyResult = strategy.Detect(data.ToArray(), (0, int.MaxValue));
            var expected = Enumerable.Range(20, 12).Where(i => i % 2 == 0)
                .Select(x => (x, new Anomaly(data[x], 1.0, Option<string>.None)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }
    }
}
