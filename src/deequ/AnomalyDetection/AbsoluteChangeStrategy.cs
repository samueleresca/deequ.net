using xdeequ.Util;

namespace xdeequ.AnomalyDetection
{
    public class AbsoluteChangeStrategy : BaseChangeStrategy
    {
        public AbsoluteChangeStrategy(Option<double> maxRateDecrease, Option<double> maxRateIncrease, int order = 1)
            : base(maxRateIncrease, order, maxRateDecrease)
        {
        }
    }
}
