using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using deequ.Util;

namespace deequ.AnomalyDetection
{
    /// <summary>
    ///
    /// </summary>
    public class RelativeRateOfChangeStrategy : BaseChangeStrategy
    {
        /// <summary>
        ///
        /// </summary>
        /// <param name="maxRateDecrease"></param>
        /// <param name="maxRateIncrease"></param>
        /// <param name="order"></param>
        public RelativeRateOfChangeStrategy(Option<double> maxRateDecrease = default,
            Option<double> maxRateIncrease = default, int order = 1) : base(maxRateIncrease, order, maxRateDecrease)
        {
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="dataSeries"></param>
        /// <param name="order"></param>
        /// <returns></returns>
        /// <exception cref="InvalidEnumArgumentException"></exception>
        public override IEnumerable<double> Diff(IEnumerable<double> dataSeries, int order)
        {
            if (order <= 0)
            {
                throw new InvalidEnumArgumentException("Order of diff cannot be zero or negative");
            }

            if (!dataSeries.Any())
            {
                return dataSeries;
            }

            List<double> valuesRight = dataSeries.Skip(order).Take(dataSeries.Count() - order).ToList();
            List<double> valuesLeft = dataSeries.Take(dataSeries.Count() - order).ToList();
            return valuesRight.Select((value, i) => value / valuesLeft[i]);
        }
    }
}
