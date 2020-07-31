using xdeequ.Util;

namespace xdeequ.AnomalyDetection
{
    class AbsoluteChangeStrategy : BaseChangeStrategy
    {
        public AbsoluteChangeStrategy(Option<double> maxRateDecrease, Option<double> maxRateIncrease, int order = 1)
            : base(maxRateIncrease, order, maxRateDecrease)
        {
        }
    }
}
