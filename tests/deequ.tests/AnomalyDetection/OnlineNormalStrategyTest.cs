using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using deequ.AnomalyDetection;
using deequ.Metrics;
using deequ.Util;
using Shouldly;
using Xunit;

namespace xdeequ.tests.AnomalyDetection
{
    public class OnlineNormalStrategyTest
    {

        public OnlineNormalStrategy _strategy;

        public static Random r = new Random(1);

        public static double[] dist = Enumerable.Range(0, 50).Select(value => NextGaussain(r)).ToArray();

        public static double[] data = dist.Select((value, index) =>
        {
            if (index >= 20 && index <= 30)
            {
                dist[index] += index + (index % 2 * -2 * index);
                return dist[index];
            }

            return value;
        }).ToArray();


        public OnlineNormalStrategyTest()
        {
            _strategy = new OnlineNormalStrategy(1.5, 1.5, 0.2);
        }

        [Fact]
        public void detect_all_anomalies_if_no_interval_specified()
        {

            var strategy = new OnlineNormalStrategy(3.5, 3.5, 0.2);

            var anomalyResult = strategy.Detect(data, (0, int.MaxValue));

            var expected = Enumerable.Range(20, 11)
                .Select(value => (value, new Anomaly(data[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();

        }

        [Fact]
        public void only_detect_anomalies_in_interval()
        {

            var anomalyResult = _strategy.Detect(data, (25, 31));

            var expected = Enumerable.Range(25, 6)
                .Select(value => (value, new Anomaly(data[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact(Skip = "")]
        public void ignore_lower_factor_if_none_is_given()
        {
            var dataCopy = data;
            var strategy = new OnlineNormalStrategy(Option<double>.None, 1.5);

            var anomalyResult = strategy.Detect(dataCopy, (0, int.MaxValue));

            var expected = Enumerable
                .Range(20, 11)
                .Where(x => x % 2 == 0)
                .Select(value => (value, new Anomaly(dataCopy[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact(Skip = "")]
        public void ignore_upper_factor_if_none_is_given()
        {

            var strategy = new OnlineNormalStrategy(1.5, Option<double>.None);

            var anomalyResult = strategy.Detect(data, (0, int.MaxValue));

            var expected = Enumerable
                .Range(21, 9)
                .Where(x => x % 2 != 0)
                .Select(value => (value, new Anomaly(data[value], 1.0)));

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void work_fine_with_empty_input()
        {

            var emptySeries = new List<double>();
            var anomalyResult = _strategy.Detect(emptySeries.ToArray(), (0, int.MaxValue));

            anomalyResult.SequenceEqual(Enumerable.Empty<(int, Anomaly)>()).ShouldBeTrue();
        }


        [Fact]
        public void detect_no_anomalies_if_factors_are_set_to_max_value()
        {
            var strategy = new OnlineNormalStrategy(Double.MaxValue, Double.MaxValue);
            var anomalyResult = strategy.Detect(data, (0, int.MaxValue));

            var expected = Enumerable.Empty<(int, Anomaly)>();

            anomalyResult.SequenceEqual(expected).ShouldBeTrue();
        }

        [Fact]
        public void calculate_variance_correctly()
        {
            var data = Enumerable.Range(1, 1000)
                .Select(value => NextGaussain(r) * (5000.0 / value)).ToArray();

            var lastPoint = _strategy.ComputeStatsAndAnomalies(data, (0, int.MaxValue)).Last();
            var mean = data.Average();
            var stdDev = StdDev(data);

            (Math.Abs(lastPoint.Mean - mean) < 0.00001).ShouldBeTrue();
            (Math.Abs(lastPoint.StdDev - stdDev) < stdDev * 0.001).ShouldBeTrue();
        }

        [Fact]
        public void ignores_anomalies_in_calculation_if_wanted()
        {
            var data = new double[] { 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0 };
            var lastPoint = _strategy.ComputeStatsAndAnomalies(data, (0, int.MaxValue)).Last();

            lastPoint.Mean.ShouldBe(1.0);
            lastPoint.StdDev.ShouldBe(0.0);
        }

        [Fact]
        public void doesn_t_ignore_anomalies_in_calculation_if_not_wanted()
        {
            var strategy = new OnlineNormalStrategy(1.5, 1.5, 0.2, false);
            var data = new[] { 1.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0 };
            var lastPoint = strategy.ComputeStatsAndAnomalies(data, (0, int.MaxValue)).Last();

            var mean = data.Average();
            var stdDev = StdDev(data);

            lastPoint.Mean.ShouldBe(mean);
            (Math.Abs(lastPoint.StdDev - stdDev) < stdDev * 0.001).ShouldBeTrue();
        }



        [Fact]
        public void throw_an_error_when_no_factor_given()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new OnlineNormalStrategy(Option<double>.None, Option<double>.None);
            });
        }

        [Fact]
        public void throw_an_error_when_factor_is_negative()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new OnlineNormalStrategy(Option<double>.None, -3.0);
            });
            Assert.Throws<ArgumentException>(() =>
            {
                new OnlineNormalStrategy(-3.0, Option<double>.None);
            });
        }

        [Fact]
        public void throw_an_error_when_percentages_are_not_in_range()
        {
            Assert.Throws<ArgumentException>(() =>
            {
                new OnlineNormalStrategy(1.5);
            });
            Assert.Throws<ArgumentException>(() =>
            {
                new OnlineNormalStrategy(-1.0);
            });
        }

        [Fact]
        public void produce_error_message_with_correct_value_and_bounds()
        {
            var result = _strategy.Detect(data, (0, int.MaxValue));

            result.ToList().ForEach(keyValue =>
            {
                (double value, double lowerBound, double upperBound) = FirstThreeDoublesFromString(keyValue.Item2.Detail.Value);

                (keyValue.Item2.Value.HasValue && value == keyValue.Item2.Value.Value).ShouldBeTrue();
                (value < lowerBound || value > upperBound).ShouldBeTrue();
            });
        }


        public static double NextGaussain(Random rand, double mean = 0.0, double stdDev = 1.0)
        {
            double u1 = 1.0 - rand.NextDouble(); //uniform(0,1] random doubles
            double u2 = 1.0 - rand.NextDouble();
            double randStdNormal = Math.Sqrt(-2.0 * Math.Log(u1)) *
                                   Math.Sin(2.0 * Math.PI * u2); //random normal(0,1)
            double randNormal =
                mean + stdDev * randStdNormal; //random normal(mean,stdDev^2)

            return randNormal;
        }

        public static double StdDev(IEnumerable<double> values)
        {
            double ret = 0;
            int count = values.Count();
            if (count > 1)
            {
                //Compute the Average
                double avg = values.Average();

                //Perform the Sum of (value-avg)^2
                double sum = values.Sum(d => (d - avg) * (d - avg));

                //Put it all together
                ret = Math.Sqrt(sum / count);
            }

            return ret;
        }

        public (double, double, double) FirstThreeDoublesFromString(string details)
        {

            var regex = new Regex("([+-]?([0-9]*[.])?[0-9]+([Ee][0-9]+)?)");
            var values = regex.Matches(details).Select(x => Double.Parse(x.ToString())).ToArray();
            return (values[0], values[1], values[2]);
        }
    }
}


