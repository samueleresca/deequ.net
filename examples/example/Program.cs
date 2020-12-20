namespace example
{
    class Program
    {
        static void Main(string[] args)
        {
            // Basic examples
            BasicExample.ExecuteSimpleVerificationSuite();
            BasicExample.ExecuteSimpleVerificationSuiteWithExternalFile();

            // Advanced examples
            IncrementalMetrics.IncrementalMetricsOnManufacturers();
            AnomalyDetection.AnomalyDetectionExample();
        }
    }
}
