using System;
using System.Collections.Generic;
using System.Linq;
using deequ;
using deequ.Checks;
using deequ.Constraints;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;


namespace xdeequ.tests.Examples
{
    [Collection("Spark instance")]
    public class BasicExample
    {
        private readonly SparkSession _session;
        private readonly ITestOutputHelper _helper;

        public BasicExample(SparkFixture fixture, ITestOutputHelper helper)
        {
            _session = fixture.Spark;
            _helper = helper;
        }

        [Fact]
        public void VerificationSuite_example()
        {
            var data = _session.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {1, "Thingy A", "awesome thing. http://thingb.com", "high", 0}),
                    new GenericRow(new object[] {2, "Thingy B", "available at http://thingb.com", null, 0}),
                    new GenericRow(new object[] {3, null, null, "low", 5}),
                    new GenericRow(new object[] {4, "Thingy D", "checkout https://thingd.ca", "low", 10}),
                    new GenericRow(new object[] {5, "Thingy E", null, "high", 12})
                },
                new StructType(new List<StructField>
                {
                    new StructField("id", new IntegerType()),
                    new StructField("productName", new StringType()),
                    new StructField("description", new StringType()),
                    new StructField("priority", new StringType()),
                    new StructField("numViews", new IntegerType()),
                }));

            var result = new VerificationSuite()
                .OnData(data)
                .AddCheck(
                    new Check(CheckLevel.Error, "integrity checks")
                        .HasSize(val => val == 5)
                        .IsComplete("id")
                        .IsUnique("id")
                        .IsComplete("productName")
                        .IsContainedIn("priority", new[] { "high", "low" })
                        .IsNonNegative("numViews")
                )
                .AddCheck(
                    new Check(CheckLevel.Warning, "distribution checks")
                        .ContainsURL("description", val => val >= .5)
                )
                .Run();

            Console.WriteLine(
                "      _                               _   _  ______  _______ \n    | |                             | \\ | ||  ____||__   __|\n  __| |  ___   ___   __ _  _   _    |  \\| || |__      | |   \n / _` | / _ \\ / _ \\ / _` || | | |   | . ` ||  __|     | |   \n| (_| ||  __/|  __/| (_| || |_| | _ | |\\  || |____    | |   \n \\__,_| \\___| \\___| \\__, | \\__,_|(_)|_| \\_||______|   |_|   \n                       | |                                  \n                       |_|                                  \n");
            if (result.Status == CheckStatus.Success) {
                _helper.WriteLine("Success");
            }
            else
            {
                _helper.WriteLine("Errors:");
                IEnumerable<ConstraintResult> constraints = result
                    .CheckResults
                    .SelectMany(pair => pair.Value.ConstraintResults)
                    .Where(c => c.Status == ConstraintStatus.Failure);

                constraints
                    .Select(constraintResult => $"{constraintResult.Metric.Value.Name} " +
                                                $"of field {constraintResult.Metric.Value.Instance} has the following error: '{constraintResult.Message.GetOrElse(string.Empty)}'")
                    .ToList().ForEach(_helper.WriteLine);
            }
        }
    }
}
