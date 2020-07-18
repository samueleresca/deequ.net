using System;
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

            if (maxRateDecrease.GetOrElse(double.MinValue) >= maxRateIncrease.GetOrElse(double.MaxValue))
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


        public IEnumerable<(int, Anomaly)> Detect(double[] dataSeries, (int, int) searchInterval)
        {
            (int start, int end) = searchInterval;

            if (start > end)
            {
                throw new ArgumentException("The start of the interval cannot be larger than the end.");
            }

            int startPoint = Math.Max(start - _order, 0);

            IEnumerable<double> data = Diff(dataSeries.Skip(startPoint).Take(end), _order);

            IEnumerable<int> indexes = Enumerable.Range(0, data.Count());

            IEnumerable<(int, Anomaly anomaly)> result = data
                .Zip(indexes, (value, index) => (value, index))
                .Where(pair => pair.value < _maxRateDecrease.GetOrElse(double.MinValue) ||
                               pair.value > _maxRateIncrease.GetOrElse(double.MaxValue))
                .Select(value =>
                {
                    Anomaly anomaly = new Anomaly(dataSeries.ElementAt(value.index + startPoint + _order), 1.0,
                        $"[AbsoluteChangeStrategy]: Change of {value.value} is not in bounds " +
                        $"[{_maxRateDecrease.GetOrElse(double.MinValue)}, " +
                        $"{_maxRateIncrease.GetOrElse(double.MaxValue)}]. Order={_order}");
                    return (value.index + startPoint + _order, anomaly);
                });

            return result;
        }


        public IEnumerable<double> Diff(IEnumerable<double> dataSeries, int order)
        {
            if (order < 0)
            {
                throw new ArgumentException("Order of diff cannot be negative");
            }

            if (order == 0 || !dataSeries.Any())
            {
                return dataSeries;
            }

            IEnumerable<double> valuesRight = dataSeries.Skip(1).Take(dataSeries.Count());
            IEnumerable<double> valuesLeft = dataSeries.Skip(0).Take(dataSeries.Count() - 1);


            double[] resultingList = new double[valuesRight.Count()];
            for (int i = 0; i < valuesRight.Count(); i++)
            {
                resultingList[i] = valuesRight.ElementAt(i) - valuesLeft.ElementAt(i);
            }

            return Diff(resultingList, order - 1);
        }
    }
}
