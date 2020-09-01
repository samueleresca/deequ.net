using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Linq;
using System.Runtime.CompilerServices;
using deequ.Util;

namespace deequ.AnomalyDetection
{
    public class OnlineNormalStrategy : IAnomalyDetectionStrategy
    {

        private readonly Option<double> _lowerDeviationFactor;
        private readonly Option<double> _upperDeviationFactor;
        public readonly double _ignoreStartPercentage = 0.1;
        private readonly bool _ignoreAnomalies = true;

        public OnlineNormalStrategy(Option<double> lowerDeviationFactor,
            Option<double> upperDeviationFactor,
            double ignoreStartPercentage = 0.1,
            bool ignoreAnomalies = true)
        {

            if (!lowerDeviationFactor.HasValue && !upperDeviationFactor.HasValue)
            {
                throw new ArgumentException("At least one factor has to be specified.");
            }
            if (lowerDeviationFactor.GetOrElse(1.0) < 0 || upperDeviationFactor.GetOrElse(1.0) < 0)
            {
                throw new ArgumentException("Factors cannot be smaller than zero.");
            }

            if (ignoreStartPercentage < 0 || ignoreStartPercentage > 1)
            {
                throw new ArgumentException("Percentage of start values to ignore must be in interval [0, 1].");
            }

            _lowerDeviationFactor = lowerDeviationFactor;
            _upperDeviationFactor = upperDeviationFactor;
            _ignoreStartPercentage = ignoreStartPercentage;
            _ignoreAnomalies = ignoreAnomalies;
        }

        public OnlineNormalStrategy(
            double ignoreStartPercentage = 0.1,
            bool ignoreAnomalies = true) :
            this(3.0, 3.0, ignoreStartPercentage, ignoreAnomalies)
        {
        }


        public IEnumerable<OnlineCalculationResult> ComputeStatsAndAnomalies(double[] dataSeries,
            (int, int) searchInterval)
        {
            var ret = new List<OnlineCalculationResult>();

            var currentMean = 0.0;
            var currentVariance = 0.0;

            var Sn = 0.0;

            var numValuesToSkip = dataSeries.Length * _ignoreStartPercentage;


            for (var i = 0; i < dataSeries.Length; i++)
            {
                var currentValue = dataSeries[i];

                var lastMean = currentMean;
                var lastVariance = currentVariance;
                var lastSn = Sn;


                if (i == 0)
                {
                    currentMean = currentValue;
                }
                else
                {
                    currentMean = lastMean + 1.0 / (i + 1) * (currentValue - lastMean);
                }

                Sn += (currentValue - lastMean) * (currentValue - currentMean);
                currentVariance = Sn / (i + 1);
                var stdDev = Math.Sqrt(currentVariance);

                var upperBound = currentMean + _upperDeviationFactor.GetOrElse(Double.MaxValue) * stdDev;
                var lowerBound = currentMean - _lowerDeviationFactor.GetOrElse(Double.MaxValue) * stdDev;

                var (searchStart, searchEnd) = searchInterval;

                if (i < numValuesToSkip ||
                    i < searchStart || i >= searchEnd ||
                    currentValue <= upperBound && currentValue >= lowerBound)
                {
                    ret.Add(new OnlineCalculationResult(currentMean, stdDev, false));
                }
                else
                {
                    if (_ignoreAnomalies)
                    {
                        // Anomaly doesn't affect mean and variance
                        currentMean = lastMean;
                        currentVariance = lastVariance;
                        Sn = lastSn;
                    }

                    ret.Add(new OnlineCalculationResult(currentMean, stdDev, true));
                }
            }

            return ret;
        }


        public IEnumerable<(int, Anomaly)> Detect(double[] dataSeries, (int, int) searchInterval)
        {
            var (searchStart, searchEnd) = searchInterval;

            if (searchStart > searchEnd)
            {
                throw new ArgumentException("The start of the interval can't be larger than the end.");
            }

            return ComputeStatsAndAnomalies(dataSeries, searchInterval)
                .Select((value, index) => new { value, index })
                .Skip(searchStart).Take(searchEnd - searchStart)
                .Where(result => result.value.IsAnomaly)
                .Select((keyValue) =>
                {
                    var lowerBound = keyValue.value.Mean - _lowerDeviationFactor.GetOrElse(Double.MaxValue) * keyValue.value.StdDev;
                    var upperBound = keyValue.value.Mean + _upperDeviationFactor.GetOrElse(Double.MaxValue) * keyValue.value.StdDev;

                    var detail =
                        $"[OnlineNormalStrategy]: Value {dataSeries[keyValue.index]} is not in bounds [{lowerBound}, {upperBound}]";

                    return (keyValue.index, new Anomaly(dataSeries[keyValue.index], 1.0, detail));
                });
        }
    }
    public class OnlineCalculationResult
    {
        public OnlineCalculationResult(in double currentMean, in double stdDev, bool isAnomaly)
        {
            Mean = currentMean;
            StdDev = stdDev;
            IsAnomaly = isAnomaly;
        }

        public double Mean { get; set; }
        public double StdDev { get; set; }
        public bool IsAnomaly { get; set; }
    }
}
