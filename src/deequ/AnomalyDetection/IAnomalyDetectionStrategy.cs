using System.Collections.Generic;

namespace xdeequ.AnomalyDetection
{
    /**
     * Interface for all strategies that spot anomalies in a series of data points.
     */
    public interface IAnomalyDetectionStrategy
    {
        /**
    * Search for anomalies in a series of data points.
    *
    * @param dataSeries     The data contained in a Vector of Doubles
    * @param searchInterval The indices between which anomalies should be detected. [a, b).
    * @return The indices of all anomalies in the interval and their corresponding wrapper object.
    */
        public IEnumerable<(int, Anomaly)> Detect(double[] dataSeries, (int, int) searchInterval);
    }
}
