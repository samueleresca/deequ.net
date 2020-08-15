using System;
using System.Collections.Generic;
using System.Linq;
using deequ;
using deequ.Checks;
using deequ.Constraints;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace examples
{
    public class BasicExample
    {
        public void ExecuteSimpleVerificationSuite()
        {

            var data = SparkSession.Builder().GetOrCreate().CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {1, "Thingy A", "awesome thing.", "high", 0}),
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

            var verificationResult = new VerificationSuite()
                .OnData(data)
                .AddCheck(
                    new Check(CheckLevel.Error, "integrity checks")
                        .HasSize(x => x == 5)
                        .IsComplete("id")
                        .IsUnique("id")
                        .IsComplete("productName")
                        .IsContainedIn("priority", new[] { "high", "low" })
                        .IsNonNegative("numViews")
                )
                .AddCheck(
                    new Check(CheckLevel.Warning, "distribution checks")
                        .ContainsURL("description", x => x == .5)
                )
                .Run();
        }


        public void ExecuteComplexVerificationSuite()
        {

            var data = SparkSession.Builder().GetOrCreate().CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {1, "Thingy A", "awesome thing.", "high", 0}),
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

            double numTitles = 0;
            double maxExpectedPhoneRatio = 0;

            VerificationResult result = new VerificationSuite()
                .OnData(data)
                .AddCheck(new Check(CheckLevel.Error)
                    .AreComplete(new []{ "customerId", "title", "impressionStart", "impressionEnd", "deviceType", "priority"})
                    .AreUnique(new []{ "customerId", "countryResidence",  "deviceType", "title"})
                    .HasApproxCountDistinct("title", value => value <= numTitles))

                .AddCheck(new Check(CheckLevel.Error)
                    .IsNonNegative("count")
                    .IsLessThan("impressionStart", "impressionEnd")
                    .IsContainedIn("priority", new[] {"hi", "lo"}))

                .AddCheck(new Check(CheckLevel.Warning)
                    .IsLessThan("impressionStart", "impressionEnd")
                    .HasCorrelation("countryResidence", "cityResidence", corr => corr >= .99)
                    .HasCorrelation("countryResidence", "zipCode", corr => corr >= .99 ))
                .Run();


            if (result.Status == CheckStatus.Success) {
                Console.WriteLine("Success");
            } else {
                Console.WriteLine("Errors:");

                IEnumerable<ConstraintResult> constraints = result
                        .CheckResults
                        .SelectMany(pair => pair.Value.ConstraintResults)
                        .Where(c=> c.Status == ConstraintStatus.Failure);

                constraints
                    .Select(constraintResult => constraintResult
                        .Message.GetOrElse(string.Empty))
                    .ToList().ForEach(Console.WriteLine);
            }
        }
    }
}
