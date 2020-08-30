using System;
using System.Collections.Generic;
using System.Linq;
using deequ.AnomalyDetection;
using deequ.Util;
using Shouldly;
using Xunit;

namespace xdeequ.tests.AnomalyDetection
{
    public class RelativeRateOfChangeStrategyTest
    {

        private readonly RelativeRateOfChangeStrategy _strategy = new RelativeRateOfChangeStrategy(0.5, 2.0);


        private readonly double[] data = Enumerable.Range(0, 50).Select(value =>
        {
            if (value < 20 || value > 30)
            {
                return 1.0;
            }

            return value % 2 == 0 ? value : 1;
        }).ToArray();


        [Fact]
        public void detect_all_anomalies_if_no_interval_specified()
        {
            var anomalyResult = _strategy.Detect(data, (0, int.MaxValue));
            var expected = Enumerable
                .Range(20, 12)
                .Select(value => (value, new Anomaly(data[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void only_detect_anomalies_in_interval()
        {
            var anomalyResult = _strategy.Detect(data, (25, 50));
            var expected = Enumerable
                .Range(25, 7)
                .Select(value => (value, new Anomaly(data[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void ignore_min_rate_if_none_is_given()
        {
            var strategy = new RelativeRateOfChangeStrategy(Option<double>.None, 1.0);
            var anomalyResult = strategy.Detect(data, (0, int.MaxValue));
            var expected = Enumerable
                .Range(20, 12)
                .Where(i => i % 2 == 0)
                .Select(value => (value, new Anomaly(data[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void ignore_max_rate_if_none_is_given()
        {
            var strategy = new RelativeRateOfChangeStrategy(0.5, Option<double>.None);
            var anomalyResult = strategy.Detect(data, (0, int.MaxValue));
            var expected = Enumerable
                .Range(21, 11)
                .Where(i => i % 2 != 0)
                .Select(value => (value, new Anomaly(data[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void detect_no_anomalies_if_rates_are_set_to_min_max_value()
        {
            var strategy = new RelativeRateOfChangeStrategy(int.MinValue, int.MaxValue);
            var anomalyResult = strategy.Detect(data, (0, int.MaxValue));
            var expected = Enumerable.Empty<(int, Anomaly)>();

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void derive_first_order_correctly()
        {
            var data = new[] {1.0, 2.0, 4.0, 1.0, 2.0, 8.0};

            IEnumerable<double> anomalyResult = _strategy.Diff(data, 1);
            var expected = new[] {2.0, 2.0, 0.25, 2.0, 4.0};

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void derive_second_order_correctly()
        {
            var data = new[] {1.0, 2.0, 4.0, 1.0, 2.0, 8.0};

            IEnumerable<double> anomalyResult = _strategy.Diff(data, 2);
            var expected = new[] {4.0, 0.5, 0.5, 8.0};

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void derive_third_order_correctly()
        {
            var data = new[] {1.0, 2.0, 4.0, 1.0, 2.0, 8.0};

            IEnumerable<double> anomalyResult = _strategy.Diff(data, 2);
            var expected = new[] {4.0, 0.5, 0.5, 8.0};

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }


        [Fact]
        public void attribute_indices_correctly_for_higher_orders_without_search_interval()
        {
            var data = new[] {0.0, 1.0, 3.0, 6.0, 18.0, 72.0};

            var strategy = new RelativeRateOfChangeStrategy(Option<double>.None, 8.0, 2);
            var anomalyResult = strategy.Detect(data, (0, int.MaxValue));

            var expected = new[]
            {
                (2, new Anomaly(3.0, 1.0)),
                (5, new Anomaly(72.0, 1.0))
            };
            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void attribute_indices_correctly_for_higher_orders_with_search_interval()
        {
            var data = new[] {0.0, 1.0, 3.0, 6.0, 18.0, 72.0};

            var strategy = new RelativeRateOfChangeStrategy(Option<double>.None, 8.0, 2);
            var anomalyResult = strategy.Detect(data, (5, 6));

            var expected = new[]
            {
                (5, new Anomaly(72.0, 1.0))
            };
            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void throw_an_error_when_rates_aren_t_ordered()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new RelativeRateOfChangeStrategy(-2.0, -3.0);
            });
        }

        [Fact]
        public void throw_an_error_when_no_maximal_rate_given()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new RelativeRateOfChangeStrategy();
            });
        }

        [Fact]
        public void work_fine_with_empty_input()
        {

            var emptySeries = Enumerable.Empty<double>().ToArray();
            var anomalyResult = _strategy.Detect(emptySeries, (0, int.MaxValue));


            anomalyResult.ShouldBeEmpty();
        }
    }
}
