using System;
using System.Collections.Generic;
using System.Linq;
using deequ.AnomalyDetection;
using deequ.Util;
using Shouldly;
using Xunit;

namespace xdeequ.tests.AnomalyDetection
{
    public class AbsoluteChangeStrategyTests
    {
        private readonly AbsoluteChangeStrategy _strategy = new AbsoluteChangeStrategy(-2.0, 2.0);

        private readonly double[] data = Enumerable.Range(0, 50).Select(value =>
        {
            if (value < 20 || value > 30)
            {
                return 1.0;
            }

            if (value % 2 == 0)
            {
                return value;
            }

            return -value;
        }).ToArray();

        [Fact]
        public void attribute_indices_correctly_for_higher_orders_with_search_interval()
        {
            double[] data = { 0.0, 1.0, 3.0, 6.0, 18.0, 72.0 };
            AbsoluteChangeStrategy strategy = new AbsoluteChangeStrategy(Option<double>.None, 8.0, 2);
            IEnumerable<(int, Anomaly)> anomalyResult = strategy.Detect(data, (5, 6));
            (int, Anomaly)[] expected = { (5, new Anomaly(72.0, 1.0, Option<string>.None)) };

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }


        [Fact]
        public void attribute_indices_correctly_for_higher_orders_without_search_interval()
        {
            double[] data = { 0.0, 1.0, 3.0, 6.0, 18.0, 72.0 };
            AbsoluteChangeStrategy strategy = new AbsoluteChangeStrategy(Option<double>.None, 8.0, 2);
            IEnumerable<(int, Anomaly)> anomalyResult = strategy.Detect(data, (0, int.MaxValue));
            (int, Anomaly)[] expected = {
                (4, new Anomaly(18.0, 1.0, Option<string>.None)), (5, new Anomaly(72.0, 1.0, Option<string>.None))
            };

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void behave_like_the_threshold_strategy_when_order_is_0()
        {
            double[] data = { 1.0, -1.0, 4.0, -7.0 };

            IEnumerable<(int, Anomaly)> anomalyResult = _strategy.Detect(data, (0, int.MaxValue));
            (int, Anomaly)[] expected = {
                (2, new Anomaly(4.0, 1.0, Option<string>.None)), (3, new Anomaly(-7.0, 1.0, Option<string>.None))
            };

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void derive_first_order_correctly()
        {
            double[] data = { 1.0, 2.0, 4.0, 1.0, 2.0, 8.0 };
            IEnumerable<double> anomalyResult = _strategy.Diff(data, 1);
            double[] expected = { 1.0, 2.0, -3.0, 1.0, 6.0 };

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void derive_second_order_correctly()
        {
            double[] data = { 1.0, 2.0, 4.0, 1.0, 2.0, 8.0 };
            IEnumerable<double> anomalyResult = _strategy.Diff(data, 2);
            double[] expected = { 1.0, -5.0, 4.0, 5.0 };

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void derive_third_order_correctly()
        {
            double[] data = { 1.0, 5.0, -10.0, 3.0, 100.0, 0.01, 0.0065 };
            IEnumerable<double> anomalyResult = _strategy.Diff(data, 3);
            double[] expected = { 47, 56, -280.99, 296.9765 };

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }


        [Fact]
        public void detect_all_anomalies_if_no_interval_specified()
        {
            IEnumerable<(int, Anomaly)> anomalyResult = _strategy.Detect(data.ToArray(), (0, int.MaxValue));
            IEnumerable<(int x, Anomaly)> expected = Enumerable.Range(20, 12)
                .Select(value => (value, new Anomaly(data[value], 1.0, Option<string>.None)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void detect_no_anomalies_if_rates_are_set_to_min_max_value()
        {
            AbsoluteChangeStrategy strategy = new AbsoluteChangeStrategy(double.MinValue, double.MaxValue);

            IEnumerable<(int, Anomaly)> anomalyResult = strategy.Detect(data.ToArray(), (0, int.MaxValue));
            List<(int, Anomaly)> expected = new List<(int, Anomaly)>();

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void ignore_max_rate_if_none_is_given()
        {
            AbsoluteChangeStrategy strategy = new AbsoluteChangeStrategy(-1.0, Option<double>.None);

            IEnumerable<(int, Anomaly)> anomalyResult = strategy.Detect(data.ToArray(), (0, int.MaxValue));
            IEnumerable<(int x, Anomaly)> expected = Enumerable.Range(21, 12).Where(i => i % 2 != 0)
                .Select(value => (value, new Anomaly(data[value], 1.0, Option<string>.None)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void ignore_min_rate_if_none_is_given()
        {
            AbsoluteChangeStrategy strategy = new AbsoluteChangeStrategy(Option<double>.None, 1.0);

            IEnumerable<(int, Anomaly)> anomalyResult = strategy.Detect(data.ToArray(), (0, int.MaxValue));
            IEnumerable<(int x, Anomaly)> expected = Enumerable.Range(20, 12)
                .Where(i => i % 2 == 0)
                .Select(value => (value, new Anomaly(data[value], 1.0, Option<string>.None)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void only_detect_anomalies_in_interval()
        {
            IEnumerable<(int, Anomaly)> anomalyResult = _strategy.Detect(data.ToArray(), (25, 50));
            IEnumerable<(int x, Anomaly)> expected = Enumerable.Range(25, 7)
                .Select(value => (value, new Anomaly(data[value], 1.0, Option<string>.None)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void throw_an_error_when_maximal_rate_given() =>
            Assert.Throws<ArgumentException>(() =>
            {
                new AbsoluteChangeStrategy(Option<double>.None, Option<double>.None);
            });

        [Fact]
        public void throw_an_error_when_rates_arent_ordered() =>
            Assert.Throws<ArgumentException>(() => { new AbsoluteChangeStrategy(-2, -3); });

        [Fact]
        public void work_fine_with_empty_input()
        {
            double[] data = { };
            IEnumerable<(int, Anomaly)> anomalyResult = _strategy.Detect(data, (0, int.MaxValue));
            anomalyResult.SequenceEqual(new (int, Anomaly)[] { }).ShouldBeTrue();
        }
    }
}
