using System.Collections.Generic;
using xdeequ.Util;

namespace xdeequ.AnomalyDetection
{
    public class Anomaly
    {
        public double Confidence;
        public Option<string> Detail;
        public Option<double> Value;

        public bool CanEqual(object that) => that is Anomaly;

        public override bool Equals(object obj)
        {
            if (obj is Anomaly anomaly)
            {
                return anomaly.Value.Value == Value.Value && anomaly.Confidence == Confidence;
            }

            return false;
        }

        public override int GetHashCode()
        {
            int prime = 31;
            int result = 1;
            result = prime * result;

            if (Value.HasValue)
            {
                result += Value.GetHashCode();
            }

            return prime * result + Confidence.GetHashCode();
        }
    }


    internal class DetectionResult
    {
        public IEnumerable<(long, Anomaly)> Anomalies;
    }
}
