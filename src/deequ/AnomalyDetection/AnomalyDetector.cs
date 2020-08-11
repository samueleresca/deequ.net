using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Util;

namespace deequ.AnomalyDetection
{
    class DataPoint<M>
    {
        public Option<M> MetricValue;
        public long Time;

        public DataPoint(long time, Option<M> metricValue)
        {
            Time = time;
            MetricValue = metricValue;
        }
    }

    class AnomalyDetector
    {
        private readonly IAnomalyDetectionStrategy _strategy;

        public AnomalyDetector(IAnomalyDetectionStrategy strategy) => _strategy = strategy;

        public DetectionResult IsNewPointAnomalous(IEnumerable<DataPoint<double>> historicalDataPoints,
            DataPoint<double> newPoint)
        {
            if (!historicalDataPoints.Any())
            {
                throw new ArgumentException("historicalDataPoints must not be empty!");
            }

            IOrderedEnumerable<DataPoint<double>> sortedDataPoints = historicalDataPoints.OrderBy(x => x.Time);

            long firstDataPointTime = sortedDataPoints.First().Time;
            long lastDataPointTime = sortedDataPoints.Last().Time;
            long newPointTime = newPoint.Time;

            if (lastDataPointTime >= newPointTime)
            {
                throw new ArgumentException(
                    "Can't decide which range to use for anomaly detection. New data point with time " +
                    $"{newPointTime} is in history range ({firstDataPointTime - lastDataPointTime})!");
            }

            IEnumerable<DataPoint<double>> allDataPoints = sortedDataPoints.Append(newPoint);
            IEnumerable<(long, Anomaly)> anomalies =
                DetectAnomaliesInHistory(allDataPoints, (newPoint.Time, long.MaxValue)).Anomalies;

            return new DetectionResult(anomalies);
        }

        private DetectionResult DetectAnomaliesInHistory(IEnumerable<DataPoint<double>> dataSeries,
            (long Time, long MaxValue) searchInterval)
        {
            (long searchStart, long searchEnd) = searchInterval;
            if (searchStart > searchEnd)
            {
                throw new ArgumentException("The first interval element has to be smaller or equal to the last.");
            }

            IEnumerable<DataPoint<double>> removeMissingValues = dataSeries.Where(x => x.MetricValue.HasValue);
            IOrderedEnumerable<DataPoint<double>> sortedSeries = removeMissingValues.OrderBy(x => x.Time);
            IEnumerable<long> sortedTimestamps = sortedSeries.Select(x => x.Time);

            int lowerBoundIndex = FindIndexForBound(sortedTimestamps.ToList(), searchStart);
            int upperBoundIndex = FindIndexForBound(sortedTimestamps.ToList(), searchEnd);

            IEnumerable<(int, Anomaly)> anomalies = _strategy.Detect(
                sortedSeries.Select(x => x.MetricValue.Value).ToArray(), (lowerBoundIndex, upperBoundIndex));


            IEnumerable<(long, Anomaly)> detectionAnomalies = anomalies
                .Select(pair => (sortedTimestamps.ToList()[pair.Item1], pair.Item2));

            return new DetectionResult(detectionAnomalies);
        }

        private int FindIndexForBound(List<long> sortedTimestamps, long boundValue)
        {
            var withinTheArray = sortedTimestamps
                .Select((value, index) => new { value, index })
                .FirstOrDefault(x => x.value >= boundValue);

            return withinTheArray?.index ?? sortedTimestamps.Count;
        }
    }
}
