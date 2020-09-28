using System;
using System.IO;
using System.Text.RegularExpressions;
using deequ;
using deequ.Checks;
using deequ.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Spark.Sql;

namespace YelpAcademicDataTests
{
    class Program
    {
        
        static void Main(string[] args)
        {
            var environmentName = Environment.GetEnvironmentVariable("ENVIRONMENT");
            
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", true, true)
                .AddJsonFile($"appsettings.{environmentName}.json", true)
                .AddEnvironmentVariables();
            
            IConfigurationRoot configuration = builder.Build(); 
            SparkSession spark = SparkSession.Builder().GetOrCreate();
            
            spark
                .SparkContext
                .SetLogLevel("WARN");
            
            DataFrame dataReview = spark
                .Read()
                .Json(configuration["dataset_review"]);
           var reviewsResult = ReviewsDataTests(dataReview);
            
           reviewsResult
            .CheckResultsAsDataFrame()
              .Show();
            
            DataFrame dataBusiness = spark
                .Read()
                .Json(configuration["dataset_business"]);

            var businessResult = BusinessDataTests(dataBusiness);
            
            businessResult
                .CheckResultsAsDataFrame()
                .Show();
        }

        private static VerificationResult ReviewsDataTests(DataFrame data)
        {
            
            VerificationResult verificationResult = new VerificationSuite()
                .OnData(data)
                .AddCheck(
                    new Check(CheckLevel.Error, "integrity checks")
                        .IsUnique("review_id")
                        .AreComplete(new[] {"review_id", "user_id", "business_id"})
                        .AreComplete(new[] {"stars", "useful", "funny", "cool"})
                        .IsComplete("date")
                        .IsContainedIn("stars", 0, 5)
                )
                .AddCheck(
                    new Check(CheckLevel.Warning, "semantic checks")
                        .ContainsURL("text", value => value <= .2)
                        .ContainsCreditCardNumber("text", value => value <= .2)
                        .ContainsEmail("text", value => value <= .2)
                        .HasMin("useful", d => d <= .2)
                        .HasMin("funny", d => d <= .2)
                        .HasMin("cool", d => d <= .2)
                )
                .Run();

            verificationResult.Debug();
            return verificationResult;
        }
        
        private static VerificationResult BusinessDataTests(DataFrame data)
        {
            Regex timeMatching = new Regex("^([0-1]?[0-9]|2[0-3]):[0-5]?[0-9]|-([0-1]?[0-9]|2[0-3]):[0-5]?[0-9]$");
            VerificationResult verificationResult = new VerificationSuite()
                .OnData(data)
                .AddCheck(
                    new Check(CheckLevel.Error, "integrity checks")
                        .IsUnique("business_id")
                        .AreComplete(new[] {"business_id", "name", "address", "city", "state", "postal_code"})
                        .IsComplete("stars")
                        .IsContainedIn("latitude", -90, 90)
                        .IsContainedIn("longitude", -180, 80)
                        .IsContainedIn("stars", 0, 5)
                        .HasPattern("hours.Monday", timeMatching, value => value >= .50 )
                        .HasPattern("hours.Tuesday", timeMatching, value => value >= .50 )
                        .HasPattern("hours.Wednesday", timeMatching, value => value >= .50 )
                        .HasPattern("hours.Thursday", timeMatching, value => value >= .50 )
                        .HasPattern("hours.Friday", timeMatching, value => value >= .50 )
                        .HasPattern("hours.Saturday", timeMatching, value => value >= .50 )
                        .HasPattern("hours.Sunday", timeMatching,value => value >= .40 )
                )
                .Run();

            verificationResult.Debug();
            return verificationResult;

        }
    }
}