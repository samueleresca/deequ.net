using deequ.Util;

namespace deequ.AnomalyDetection
{
    class AbsoluteChangeStrategy : BaseChangeStrategy
    {
        public AbsoluteChangeStrategy(Option<double> maxRateDecrease, Option<double> maxRateIncrease, int order = 1)
            : base(maxRateIncrease, order, maxRateDecrease)
        {
        }
    }
}
