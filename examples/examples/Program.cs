using System;
namespace examples
{
    class Program
    {
        static void Main(string[] args)
        {
            BasicExample.ExecuteSimpleVerificationSuite();
            IncrementalMetrics.IncrementalChangesOnManufacturers();
            //new BasicExample().ExecuteComplexVerificationSuite();
        }
    }
}
