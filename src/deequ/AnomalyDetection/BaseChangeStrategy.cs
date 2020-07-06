using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using xdeequ.Util;

namespace xdeequ.AnomalyDetection
{
    public abstract class BaseChangeStrategy : IAnomalyDetectionStrategy
    {
        private readonly Option<double> _maxRateDecrease;
        private readonly Option<double> _maxRateIncrease;
        private readonly int _order;

        public BaseChangeStrategy(Option<double> maxRateIncrease, int order, Option<double> maxRateDecrease)
        {
            if (!maxRateDecrease.HasValue && !maxRateIncrease.HasValue)
            {
                throw new ArgumentException(
                    "At least one of the two limits (maxRateDecrease or maxRateIncrease) has to be specified.");
            }

            if (maxRateDecrease.GetOrElse(Double.MinValue) >= maxRateIncrease.GetOrElse(Double.MaxValue))
            {
                throw new ArgumentException(
                    "The maximal rate of increase has to be bigger than the maximal rate of decrease.");
            }

            if (order < 0)
            {
                throw new ArgumentException(
                    "Order of derivative cannot be negative.");
            }

            _maxRateIncrease = maxRateIncrease;
            _order = order;
            _maxRateDecrease = maxRateDecrease;
        }


        private IEnumerable<double> Diff(IEnumerable<double> dataSeries, int order)
        {
            if (order < 0)
            {
                throw new ArgumentException("Order of diff cannot be negative");
            }

            if (order == 0 || !dataSeries.Any())
            {
                return dataSeries;
            }

            var valuesRight = dataSeries.Skip(1).Take(dataSeries.Count());
            var valuesLeft = dataSeries.Skip(0).Take(dataSeries.Count() - 1);


            double[] resultingList = new double[valuesRight.Count()];
            for (int i = 0; i < valuesRight.Count(); i++)
            {
                resultingList[i] = valuesRight.ElementAt(i) - valuesLeft.ElementAt(i);
            }

            return Diff(resultingList, order - 1);
        }


        public IEnumerable<(int, Anomaly)> Detect(double[] dataSeries, (int, int) searchInterval)
        {
            (int start, int end) = searchInterval;

            if (start > end)
            {
                throw new ArgumentException("The start of the interval cannot be larger than the end.");
            }

            var startPoint = Math.Max(start - _order, 0);

            var data = Diff(dataSeries.Skip(startPoint).Take(end), _order);

            int index = 0;
            var result = data.Where(d => d < _maxRateDecrease.GetOrElse(Double.MaxValue) ||
                                         d > _maxRateIncrease.GetOrElse(Double.MaxValue))
                .Select(value =>
                {
                    var anomaly = new Anomaly(dataSeries.ElementAt(index + startPoint + _order), 1.0,
                        "[AbsoluteChangeStrategy]: Change of $change is not in bounds " +
                        $"[{_maxRateDecrease.GetOrElse(Double.MinValue)}, " +
                        $"{_maxRateIncrease.GetOrElse(Double.MaxValue)}]. Order={_order}");
                    return (index + startPoint + _order, anomaly);
                });

            return result;
        }
    }
}
