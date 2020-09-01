using System.Collections.Generic;
using deequ.Util;

namespace deequ.AnomalyDetection
{
    public class Anomaly
    {
        public readonly double Confidence;
        public Option<string> Detail;
        public Option<double> Value;

        public Anomaly(Option<double> value, double confidence, Option<string> detail = default)
        {
            Confidence = confidence;
            Detail = detail;
            Value = value;
        }

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


    class DetectionResult
    {
        public IEnumerable<(long, Anomaly)> Anomalies;

        public DetectionResult(IEnumerable<(long, Anomaly)> anomalies) => Anomalies = anomalies;
    }
}
