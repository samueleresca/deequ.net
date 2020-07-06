using System;
using System.Collections.Generic;
using System.Linq;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.AnomalyDetection
{

    public class DataPoint<M>
    {
        public long Time;
        public Option<M> MetricValue;

        public DataPoint(long time, Option<M> metricValue)
        {
            Time = time;
            MetricValue = metricValue;
        }

    }
    public class AnomalyDetector
    {
        private readonly IAnomalyDetectionStrategy _strategy;

        public AnomalyDetector(IAnomalyDetectionStrategy strategy)
        {
            _strategy = strategy;
        }

        public DetectionResult IsNewPointAnomalous(IEnumerable<DataPoint<double>> historicalDataPoints,
            DataPoint<double> newPoint)
        {
            if (!historicalDataPoints.Any())
            {
                throw new ArgumentException("historicalDataPoints must not be empty!");
            }

            var sortedDataPoints = historicalDataPoints.OrderBy(x => x.Time);

            var firstDataPointTime = sortedDataPoints.First().Time;
            var lastDataPointTime = sortedDataPoints.Last().Time;
            var newPointTime = newPoint.Time;

            if (lastDataPointTime >= newPointTime)
            {
                throw new ArgumentException("Can't decide which range to use for anomaly detection. New data point with time " +
                                            $"{newPointTime} is in history range ({firstDataPointTime - lastDataPointTime})!");
            }

            var allDataPoints = sortedDataPoints.Append(newPoint);
            var anomalies = DetectAnomaliesInHistory(allDataPoints, (newPoint.Time, long.MaxValue)).Anomalies;

            return new DetectionResult(anomalies);

        }

        private DetectionResult DetectAnomaliesInHistory(IEnumerable<DataPoint<double>> dataSeries, (long Time, long MaxValue) searchInterval)
        {

            (long searchStart, long searchEnd) = searchInterval;
            if (searchStart > searchEnd)
            {
                throw new ArgumentException("The first interval element has to be smaller or equal to the last.");
            }

            var removeMissingValues = dataSeries.Where(x => x.MetricValue.HasValue);
            var sortedSeries = removeMissingValues.OrderBy(x => x.Time);
            var sortedTimestamps = sortedSeries.Select(x => x.Time);

            var lowerBoundIndex = FindIndexForBound(sortedTimestamps.ToList(), searchStart);
            var upperBoundIndex = FindIndexForBound(sortedTimestamps.ToList(), searchEnd);

            var anomalies = _strategy.Detect(
                sortedSeries.Select(x => x.MetricValue.Value).ToArray(), (lowerBoundIndex, upperBoundIndex));


            var detectionAnomalies = anomalies
                .Select(pair => (sortedTimestamps.ToList()[pair.Item1], pair.Item2));

            return new DetectionResult(detectionAnomalies);
        }

        private int FindIndexForBound(List<long> sortedTimestamps, long boundValue)
        {
            return sortedTimestamps.IndexOf(boundValue);
        }
    }
}
