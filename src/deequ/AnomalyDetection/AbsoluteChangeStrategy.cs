using deequ.Util;

namespace deequ.AnomalyDetection
{
    public class AbsoluteChangeStrategy : BaseChangeStrategy
    {
        public AbsoluteChangeStrategy(Option<double> maxRateDecrease = default, Option<double> maxRateIncrease = default, int order = 1)
            : base(maxRateIncrease, order, maxRateDecrease)
        {
        }
    }
}
