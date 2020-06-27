using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Checks;
using xdeequ.Constraints;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using Xunit.Abstractions;

namespace xdeequ.tests.Checks
{
    [Collection("Spark instance")]
    public class CheckTests
    {
        public CheckTests(SparkFixture fixture, ITestOutputHelper helper)
        {
            _session = fixture.Spark;
            _helper = helper;
        }

        private readonly SparkSession _session;

        private static ITestOutputHelper _helper;


        public static AnalyzerContext RunChecks(DataFrame data, Check check, Check[] checks)
        {
            IEnumerable<IAnalyzer<IMetric>> analyzers = check.RequiredAnalyzers()
                .Concat(checks.SelectMany(x => x.RequiredAnalyzers())).AsEnumerable();

            return new AnalysisRunBuilder()
                .OnData(data)
                .AddAnalyzers(analyzers)
                .Run();
        }

        public static void AssertEvaluatesTo(Check check, AnalyzerContext analyzerContext, CheckStatus checkStatus)
        {
            CheckResult checkResult = check.Evaluate(analyzerContext);
            if (checkResult.Status == CheckStatus.Error)
            {
                _helper.WriteLine(
                    $"Check {check.Description} failed because {checkResult.ConstraintResults.FirstOrDefault()?.Message.Value}");
            }

            checkResult.Status.ShouldBe(checkStatus);
        }

        [Fact]
        public void should_check_credit_cards()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"9999888877776666", "invalid"}),
                new GenericRow(new object[] {"6011 1111 1111 1117", "valid"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField(col, new StringType()), new StructField("type", new StringType())
                });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsCreditCardNumber(col, _ => _ == .5, Option<string>.None);

            Check checkWithFilter = new Check(CheckLevel.Error, "some description")
                .ContainsCreditCardNumber(col, _ => _ == 1, Option<string>.None).Where("type = 'valid'");

            AnalyzerContext context =
                RunChecks(df, check, new[] {checkWithFilter});

            AssertEvaluatesTo(check, context, CheckStatus.Success);
            AssertEvaluatesTo(checkWithFilter, context, CheckStatus.Success);
        }

        [Fact]
        public void should_check_DataType()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"2", "integral"}), new GenericRow(new object[] {"1.0", "fractional"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField("value", new StringType()), new StructField("type", new StringType())
                });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .HasDataType("value", ConstrainableDataTypes.Integral, _ => _ == .5, Option<string>.None);

            Check checkWithFilter = new Check(CheckLevel.Error, "some description")
                .HasDataType("value", ConstrainableDataTypes.Integral, _ => _ == 1, Option<string>.None)
                .Where("type = 'integral'");

            AnalyzerContext context =
                RunChecks(df, check, new[] {checkWithFilter});

            AssertEvaluatesTo(check, context, CheckStatus.Success);
            AssertEvaluatesTo(checkWithFilter, context, CheckStatus.Success);
        }


        [Fact]
        public void should_check_email()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"someone@somewhere.org", "valid"}),
                new GenericRow(new object[] {"someone@else", "invalid"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField(col, new StringType()), new StructField("type", new StringType())
                });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsEmail(col, _ => _ == .5, Option<string>.None);

            Check checkWithFilter = new Check(CheckLevel.Error, "some description")
                .ContainsEmail(col, _ => _ == 1, Option<string>.None).Where("type = 'valid'");

            AnalyzerContext context =
                RunChecks(df, check, new[] {checkWithFilter});

            AssertEvaluatesTo(check, context, CheckStatus.Success);
            AssertEvaluatesTo(checkWithFilter, context, CheckStatus.Success);
        }

        [Fact]
        public void should_check_SSN()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"111-05-1130", "valid"}),
                new GenericRow(new object[] {"something else", "invalid"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField(col, new StringType()), new StructField("type", new StringType())
                });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsSSN(col, _ => _ == .5, Option<string>.None);

            Check checkWithFilter = new Check(CheckLevel.Error, "some description")
                .ContainsSSN(col, _ => _ == 1, Option<string>.None).Where("type = 'valid'");

            AnalyzerContext context =
                RunChecks(df, check, new[] {checkWithFilter});

            AssertEvaluatesTo(check, context, CheckStatus.Success);
            AssertEvaluatesTo(checkWithFilter, context, CheckStatus.Success);
        }

        [Fact]
        public void should_check_URL()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"https://www.example.com/foo/?bar=baz&inga=42&quux", "valid"}),
                new GenericRow(new object[] {"http:// shouldfail.com", "invalid"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField(col, new StringType()), new StructField("type", new StringType())
                });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsURL(col, _ => _ == .5, Option<string>.None);

            Check checkWithFilter = new Check(CheckLevel.Error, "some description")
                .ContainsURL(col, _ => _ == 1, Option<string>.None).Where("type = 'valid'");

            AnalyzerContext context =
                RunChecks(df, check, new[] {checkWithFilter});

            AssertEvaluatesTo(check, context, CheckStatus.Success);
            AssertEvaluatesTo(checkWithFilter, context, CheckStatus.Success);
        }

        [Fact]
        public void should_correctly_evaluate_greater_than_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            Check greaterThanCheck = new Check(CheckLevel.Error, "a")
                .IsGreaterThan("att2", "att1", Option<string>.None)
                .Where("item > 3");

            CheckWithLastConstraintFilterable incorrectGreaterThanCheck = new Check(CheckLevel.Error, "a")
                .IsGreaterThan("att2", "att1", Option<string>.None);

            CheckWithLastConstraintFilterable greaterThanCheckWithCustomAssertionFunction
                = new Check(CheckLevel.Error, "a")
                    .IsGreaterThan("att2", "att1", _ => _ == 0.5, Option<string>.None);

            CheckWithLastConstraintFilterable incorrectGreaterThanCheckWithCustomAssertionFunction =
                new Check(CheckLevel.Error, "a")
                    .IsGreaterThan("att2", "att1", _ => _ == 0.4, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, greaterThanCheck,
                    new[]
                    {
                        incorrectGreaterThanCheck, greaterThanCheckWithCustomAssertionFunction,
                        incorrectGreaterThanCheckWithCustomAssertionFunction
                    });

            AssertEvaluatesTo(greaterThanCheck, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectGreaterThanCheck, context, CheckStatus.Error);
            AssertEvaluatesTo(greaterThanCheckWithCustomAssertionFunction, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectGreaterThanCheckWithCustomAssertionFunction, context, CheckStatus.Error);
        }

        [Fact]
        public void should_correctly_evaluate_greater_than_or_equal_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            Check greaterThanCheck = new Check(CheckLevel.Error, "a")
                .IsGreaterOrEqualTo("att3", "att1", Option<string>.None).Where("item > 3");

            CheckWithLastConstraintFilterable incorrectGreatThanCheck = new Check(CheckLevel.Error, "a")
                .IsGreaterOrEqualTo("att3", "att1", Option<string>.None);

            CheckWithLastConstraintFilterable greaterThanCheckWithCustomAssertionFunction
                = new Check(CheckLevel.Error, "a")
                    .IsGreaterOrEqualTo("att3", "att1", _ => _ == 0.5, Option<string>.None);

            CheckWithLastConstraintFilterable incorrectGreatThanCheckWithCustomAssertionFunction =
                new Check(CheckLevel.Error, "a")
                    .IsGreaterOrEqualTo("att3", "att1", _ => _ == 0.4, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, greaterThanCheck,
                    new[]
                    {
                        incorrectGreatThanCheck, greaterThanCheckWithCustomAssertionFunction,
                        incorrectGreatThanCheckWithCustomAssertionFunction
                    });

            AssertEvaluatesTo(greaterThanCheck, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectGreatThanCheck, context, CheckStatus.Error);
            AssertEvaluatesTo(greaterThanCheckWithCustomAssertionFunction, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectGreatThanCheckWithCustomAssertionFunction, context, CheckStatus.Error);
        }

        [Fact(Skip = "Implement HasApproxQuantile")]
        public void should_correctly_evaluate_HasApproxQuantile_constraints()
        {
        }

        [Fact]
        public void should_correctly_evaluate_less_than_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            Check lessThanCheck = new Check(CheckLevel.Error, "a")
                .IsLessThan("att1", "att2", Option<string>.None)
                .Where("item > 3");

            CheckWithLastConstraintFilterable incorrectLessThanCheck = new Check(CheckLevel.Error, "a")
                .IsLessThan("att1", "att2", Option<string>.None);

            CheckWithLastConstraintFilterable lessThanCheckWithCustomAssertionFunction
                = new Check(CheckLevel.Error, "a")
                    .IsLessThan("att1", "att2", _ => _ == 0.5, Option<string>.None);

            CheckWithLastConstraintFilterable incorrectLessThanCheckWithCustomAssertionFunction =
                new Check(CheckLevel.Error, "a")
                    .IsLessThan("att1", "att2", _ => _ == 0.4, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, lessThanCheck,
                    new[]
                    {
                        incorrectLessThanCheck, lessThanCheckWithCustomAssertionFunction,
                        incorrectLessThanCheckWithCustomAssertionFunction
                    });

            AssertEvaluatesTo(lessThanCheck, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectLessThanCheck, context, CheckStatus.Error);
            AssertEvaluatesTo(lessThanCheckWithCustomAssertionFunction, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectLessThanCheckWithCustomAssertionFunction, context, CheckStatus.Error);
        }

        [Fact]
        public void should_correctly_evaluate_less_than_or_equal_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            Check lessThanCheck = new Check(CheckLevel.Error, "a")
                .IsLessThanOrEqualTo("att1", "att3", Option<string>.None).Where("item > 3");

            CheckWithLastConstraintFilterable incorrectLessThanCheck = new Check(CheckLevel.Error, "a")
                .IsLessThanOrEqualTo("att1", "att3", Option<string>.None);

            CheckWithLastConstraintFilterable lessThanCheckWithCustomAssertionFunction
                = new Check(CheckLevel.Error, "a")
                    .IsLessThanOrEqualTo("att1", "att3", _ => _ == 0.5, Option<string>.None);

            CheckWithLastConstraintFilterable incorrectLessThanCheckWithCustomAssertionFunction =
                new Check(CheckLevel.Error, "a")
                    .IsLessThanOrEqualTo("att1", "att3", _ => _ == 0.4, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, lessThanCheck,
                    new[]
                    {
                        incorrectLessThanCheck, lessThanCheckWithCustomAssertionFunction,
                        incorrectLessThanCheckWithCustomAssertionFunction
                    });

            AssertEvaluatesTo(lessThanCheck, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectLessThanCheck, context, CheckStatus.Error);
            AssertEvaluatesTo(lessThanCheckWithCustomAssertionFunction, context, CheckStatus.Success);
            AssertEvaluatesTo(incorrectLessThanCheckWithCustomAssertionFunction, context, CheckStatus.Error);
        }

        [Fact]
        public void should_correctly_evaluate_mean_constraints()
        {
            CheckWithLastConstraintFilterable meanCheck = new Check(CheckLevel.Error, "a")
                .HasMean("att1", _ => _ == 3.5, Option<string>.None);

            Check meanCheckFiltered = new Check(CheckLevel.Error, "a")
                .HasMean("att1", _ => _ == 5.0, Option<string>.None).Where("att2 > 0");

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDfWithNumericValues(_session), meanCheck, new[] {meanCheckFiltered});

            AssertEvaluatesTo(meanCheck, context, CheckStatus.Success);
            AssertEvaluatesTo(meanCheckFiltered, context, CheckStatus.Success);
        }


        [Fact]
        public void should_correctly_evaluate_non_negative_and_positive_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            CheckWithLastConstraintFilterable isNonNegative = new Check(CheckLevel.Error, "a")
                .IsNonNegative("item", Option<string>.None);

            CheckWithLastConstraintFilterable isPositive = new Check(CheckLevel.Error, "a")
                .IsPositive("att3", Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, isNonNegative, new[] {isPositive});

            AssertEvaluatesTo(isNonNegative, context, CheckStatus.Success);
            AssertEvaluatesTo(isPositive, context, CheckStatus.Success);
        }

        [Fact]
        public void should_correctly_evaluate_range_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithDistinctValues(_session);

            CheckWithLastConstraintFilterable rangeCheck = new Check(CheckLevel.Error, "a")
                .IsContainedIn("att1", new[] {"a", "b", "c"});

            CheckWithLastConstraintFilterable inCorrectRangeCheck = new Check(CheckLevel.Error, "a")
                .IsContainedIn("att1", new[] {"a", "b"});

            CheckWithLastConstraintFilterable inCorrectRangeCheckWithCustomAssertionFunction =
                new Check(CheckLevel.Error, "a")
                    .IsContainedIn("att1", new[] {"a"}, _ => _ == 0.5);

            AnalyzerContext context =
                RunChecks(df, rangeCheck, new[] {inCorrectRangeCheck, inCorrectRangeCheckWithCustomAssertionFunction});

            AssertEvaluatesTo(rangeCheck, context, CheckStatus.Success);
            AssertEvaluatesTo(inCorrectRangeCheck, context, CheckStatus.Error);
            AssertEvaluatesTo(inCorrectRangeCheckWithCustomAssertionFunction, context, CheckStatus.Success);


            CheckWithLastConstraintFilterable numericRangeCheck1 = new Check(CheckLevel.Error, "nr1")
                .IsContainedIn("att2", 0, 7, Option<string>.None);

            CheckWithLastConstraintFilterable numericRangeCheck2 = new Check(CheckLevel.Error, "nr2")
                .IsContainedIn("att2", 1, 7, Option<string>.None);

            CheckWithLastConstraintFilterable numericRangeCheck3 = new Check(CheckLevel.Error, "nr3")
                .IsContainedIn("att2", 0, 6, Option<string>.None);

            CheckWithLastConstraintFilterable numericRangeCheck4 = new Check(CheckLevel.Error, "nr4")
                .IsContainedIn("att2", 0, 7, Option<string>.None, includeLowerBound: false, includeUpperBound: false);

            CheckWithLastConstraintFilterable numericRangeCheck5 = new Check(CheckLevel.Error, "nr5")
                .IsContainedIn("att2", -1, 8, Option<string>.None, includeLowerBound: false, includeUpperBound: false);

            CheckWithLastConstraintFilterable numericRangeCheck6 = new Check(CheckLevel.Error, "nr6")
                .IsContainedIn("att2", 0, 7, Option<string>.None, includeLowerBound: true, includeUpperBound: false);

            CheckWithLastConstraintFilterable numericRangeCheck7 = new Check(CheckLevel.Error, "nr7")
                .IsContainedIn("att2", 0, 8, Option<string>.None, includeLowerBound: true, includeUpperBound: false);

            CheckWithLastConstraintFilterable numericRangeCheck8 = new Check(CheckLevel.Error, "nr8")
                .IsContainedIn("att2", 0, 7, Option<string>.None, includeLowerBound: false, includeUpperBound: true);

            CheckWithLastConstraintFilterable numericRangeCheck9 = new Check(CheckLevel.Error, "nr0")
                .IsContainedIn("att2", -1, 7, Option<string>.None, includeLowerBound: false, includeUpperBound: true);


            AnalyzerContext numericRangeResults = RunChecks(FixtureSupport.GetDfWithNumericValues(_session),
                numericRangeCheck1,
                new[]
                {
                    numericRangeCheck2, numericRangeCheck3, numericRangeCheck4, numericRangeCheck5,
                    numericRangeCheck6, numericRangeCheck7, numericRangeCheck8, numericRangeCheck9
                });


            AssertEvaluatesTo(numericRangeCheck1, numericRangeResults, CheckStatus.Success);
            AssertEvaluatesTo(numericRangeCheck2, numericRangeResults, CheckStatus.Error);
            AssertEvaluatesTo(numericRangeCheck3, numericRangeResults, CheckStatus.Error);
            AssertEvaluatesTo(numericRangeCheck4, numericRangeResults, CheckStatus.Error);
            AssertEvaluatesTo(numericRangeCheck5, numericRangeResults, CheckStatus.Success);
            AssertEvaluatesTo(numericRangeCheck6, numericRangeResults, CheckStatus.Error);
            AssertEvaluatesTo(numericRangeCheck7, numericRangeResults, CheckStatus.Success);
            AssertEvaluatesTo(numericRangeCheck8, numericRangeResults, CheckStatus.Error);
            AssertEvaluatesTo(numericRangeCheck9, numericRangeResults, CheckStatus.Success);
        }


        [Fact]
        public void should_correctly_return_the_correct_check_status_for_entropy_constraints()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);
            double expectedValue = -(0.75 * Math.Log(0.75) + 0.25 * Math.Log(0.25));

            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "group-1")
                .HasEntropy("att1", _ => _ == expectedValue, Option<string>.None);

            Check check2 = new Check(CheckLevel.Error, "group-1")
                .HasEntropy("att1", _ => _ == 0, Option<string>.None).Where("att2 = 'c'");

            CheckWithLastConstraintFilterable check3 = new Check(CheckLevel.Error, "group-1")
                .HasEntropy("att1", _ => _ != expectedValue, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check1, new[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Success);
            AssertEvaluatesTo(check3, context, CheckStatus.Error);
        }

        [Fact]
        public void should_correctly_return_the_correct_check_status_for_mutual_information_constraints()
        {
            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "check")
                .HasMutualInformation("att1", "att2", _ => Math.Abs(_ - 0.5623) < 0.0001, Option<string>.None);

            Check checkWithFilter = new Check(CheckLevel.Error, "check")
                .HasMutualInformation("att1", "att2", _ => _ == 0, Option<string>.None).Where("att2 = 'c'");

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDFFull(_session), check, new[] {checkWithFilter});

            AssertEvaluatesTo(check, context, CheckStatus.Success);
            AssertEvaluatesTo(checkWithFilter, context, CheckStatus.Success);
        }

        [Fact]
        public void should_correctly_yield_correct_results_for_basic_stats()
        {
            // TODO: missing HasApproxQuantile HasCountDistinct, HasCorrelation
            Check check = new Check(CheckLevel.Error, "a description");
            DataFrame dfNumeric = FixtureSupport.GetDfWithNumericValues(_session);
            DataFrame dfInformative = FixtureSupport.GetDfWithConditionallyInformativeColumns(_session);
            DataFrame dfUninformative = FixtureSupport.GetDfWithConditionallyUninformativeColumns(_session);

            CheckWithLastConstraintFilterable hasMinimum = check.HasMin("att1", _ => _ == 1.0, Option<string>.None);
            CheckWithLastConstraintFilterable hasMaximum = check.HasMax("att1", _ => _ == 6.0, Option<string>.None);
            CheckWithLastConstraintFilterable hasMean = check.HasMean("att1", _ => _ == 3.5, Option<string>.None);
            CheckWithLastConstraintFilterable hasSum = check.HasSum("att1", _ => _ == 21.0, Option<string>.None);
            CheckWithLastConstraintFilterable hasStandardDeviation =
                check.HasStandardDeviation("att1", _ => _ == 1.707825127659933, Option<string>.None);

            /* Analysis numericAnalysis = new Analysis().AddAnalyzers(new IAnalyzer<IMetric>[]
             {
                 hasMinimum,
                 Initializers.Maximum("att1"),
                 Initializers.Mean("att1"),
                 Initializers.Sum("att1"),
                 Initializers.StandardDeviation("att1")
             });*/

            AnalyzerContext context = RunChecks(dfNumeric, hasMinimum,
                new Check[] {hasMaximum, hasMean, hasSum, hasStandardDeviation});

            AssertEvaluatesTo(hasMinimum, context, CheckStatus.Success);
            AssertEvaluatesTo(hasMaximum, context, CheckStatus.Success);
            AssertEvaluatesTo(hasMean, context, CheckStatus.Success);
            AssertEvaluatesTo(hasSum, context, CheckStatus.Success);
            AssertEvaluatesTo(hasStandardDeviation, context, CheckStatus.Success);
        }

        [Fact]
        public void should_fail_on_mixed_data_for_E_Mail_pattern_with_default_assertion()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"someone@somewhere.org"}),
                new GenericRow(new object[] {"someone@else"})
            };

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable hasPattern = new Check(CheckLevel.Error, "some description")
                .HasPattern(col, Patterns.Email, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, hasPattern, new Check[] { });

            AssertEvaluatesTo(hasPattern, context, CheckStatus.Error);
        }

        [Fact]
        public void should_fail_on_mixed_data_for_URL_pattern_with_default_assertion()
        {
            string col = "some";
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"http:// foo.com/blah_blah"}),
                new GenericRow(new object[] {"http://foo.com/blah_blah"})
            };

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable hasPattern = new Check(CheckLevel.Error, "some description")
                .HasPattern(col, Patterns.Url, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, hasPattern, new Check[] { });

            AssertEvaluatesTo(hasPattern, context, CheckStatus.Error);
        }

        [Fact]
        public void should_find_credit_card_numbers_embedded_in_text()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"My credit card number is: 4111-1111-1111-1111."})
            };

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsCreditCardNumber(col, _ => _ == 1, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check, new Check[] { });

            AssertEvaluatesTo(check, context, CheckStatus.Success);
        }

        [Fact]
        public void should_find_emails_embedded_in_text()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"Please contact me at someone@somewhere.org, thank you."})
            };

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsEmail(col, _ => _ == 1, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check, new Check[] { });

            AssertEvaluatesTo(check, context, CheckStatus.Success);
        }

        [Fact]
        public void should_find_SSN_embedded_in_text()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"My SSN is 111-05-1130, thanks."})
            };

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsSSN(col, _ => _ == 1, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check, new Check[] { });

            AssertEvaluatesTo(check, context, CheckStatus.Success);
        }

        [Fact]
        public void should_find_URL_embedded_in_text()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[]
                {
                    "Hey, please have a look at https://www.example.com/foo/?bar=baz&inga=42&quux!"
                })
            };

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable check = new Check(CheckLevel.Error, "some description")
                .ContainsURL(col, _ => _ == 1, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check, new Check[] { });

            AssertEvaluatesTo(check, context, CheckStatus.Success);
        }

        [Fact]
        public void should_return_the_correct_status_for_any_completeness()
        {
            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "group-1")
                .AreAnyComplete(new[] {"item", "att1"}, Option<string>.None)
                .HaveAnyCompleteness(new[] {"item", "att1"}, _ => _ == 1.0, Option<string>.None);

            CheckWithLastConstraintFilterable check2 = new Check(CheckLevel.Error, "group-2-E")
                .HaveAnyCompleteness(new[] {"att1", "att2"}, _ => _ > 0.917, Option<string>.None);

            CheckWithLastConstraintFilterable check3 = new Check(CheckLevel.Warning, "group-2-W")
                .HaveAnyCompleteness(new[] {"att1", "att2"}, _ => _ > 0.917, Option<string>.None);


            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDFMissing(_session), check1, new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Warning);
        }

        [Fact]
        public void should_return_the_correct_status_for_combined_completeness()
        {
            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "group-1")
                .AreComplete(new[] {"item", "att1"}, Option<string>.None)
                .HaveCompleteness(new[] {"item", "att1"}, _ => _ == 1.0, Option<string>.None);

            CheckWithLastConstraintFilterable check2 = new Check(CheckLevel.Error, "group-2-E")
                .HaveCompleteness(new[] {"item", "att1", "att2"}, _ => _ > 0.8, Option<string>.None);

            CheckWithLastConstraintFilterable check3 = new Check(CheckLevel.Warning, "group-2-W")
                .HaveCompleteness(new[] {"item", "att1", "att2"}, _ => _ > 0.8, Option<string>.None);


            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDfCompleteAndInCompleteColumns(_session), check1,
                    new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Warning);
        }

        [Fact]
        public void should_return_the_correct_status_for_completness()
        {
            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None)
                .HasCompleteness("att1", _ => _ == 1.0, Option<string>.None);

            CheckWithLastConstraintFilterable check2 = new Check(CheckLevel.Error, "group-2-E")
                .HasCompleteness("att2", _ => _ > 0.8, Option<string>.None);

            CheckWithLastConstraintFilterable check3 = new Check(CheckLevel.Warning, "group-2-W")
                .HasCompleteness("att2", _ => _ > 0.8, Option<string>.None);


            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDfCompleteAndInCompleteColumns(_session), check1,
                    new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Warning);
        }

        [Fact]
        public void should_return_the_correct_status_for_conditional_column_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            Check check1 = new Check(CheckLevel.Error, "group-1")
                .Satisfies("att1 < att2", "rule1", Option<string>.None).Where("att1 > 3");

            Check check2 = new Check(CheckLevel.Error, "group-2")
                .Satisfies("att2 > 0", "rule2", Option<string>.None).Where("att1 > 0");

            Check check3 = new Check(CheckLevel.Error, "group-1")
                .Satisfies("att2 > 0", "rule3", _ => _ == .5, Option<string>.None).Where("att1 > 0");

            AnalyzerContext context =
                RunChecks(df, check1, new[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Success);
        }

        [Fact]
        public void should_return_the_correct_status_for_constraints()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            long numberOfRows = df.Count();

            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "group-1")
                .Satisfies("att1 > 0", "rule1", Option<string>.None);

            CheckWithLastConstraintFilterable check2 = new Check(CheckLevel.Error, "group-2-to-fail")
                .Satisfies("att1 > 3", "rule2", Option<string>.None);

            CheckWithLastConstraintFilterable check3 = new Check(CheckLevel.Error, "group-2-to-succeed")
                .Satisfies("att1 > 3", "rule3", _ => _ == .5, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check1, new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Success);
        }

        [Fact]
        public void should_return_the_correct_status_for_distinctness()
        {
            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "distinctness-check")
                .HasDistinctness(new[] {"att1"}, _ => _ == 3.0 / 5, Option<string>.None)
                .HasDistinctness(new[] {"att1"}, _ => _ == 2.0 / 3, Option<string>.None).Where("att2 is not null")
                .HasDistinctness(new[] {"att1", "att2"}, _ => _ == 4.0 / 6, Option<string>.None)
                .HasDistinctness(new[] {"att2"}, _ => _ == 1.0, Option<string>.None);

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDfWithDistinctValues(_session), check1, new Check[] { });

            CheckResult result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Error);
            IEnumerable<ConstraintStatus> constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(3).First().ShouldBe(ConstraintStatus.Failure);
        }

        [Fact]
        public void should_return_the_correct_status_for_has_uniqueness()
        {
            Check check1 = new Check(CheckLevel.Error, "group-1-u")
                .HasUniqueness("nonUnique", fraction => fraction == .5)
                .HasUniqueness("nonUnique", fraction => fraction < .6)
                .HasUniqueness(new[] {"halfUniqueCombinedWithNonUnique", "nonUnique"}, fraction => fraction == .5)
                .HasUniqueness(new[] {"onlyUniqueWithOtherNonUnique", "nonUnique"}, Check.IsOne)
                .HasUniqueness("unique", Check.IsOne)
                .HasUniqueness("uniqueWithNulls", Check.IsOne)
                .HasUniqueness(new[] {"nonUnique", "halfUniqueCombinedWithNonUnique"}, Check.IsOne)
                .Where("nonUnique > 0")
                .HasUniqueness(new[] {"nonUnique", "halfUniqueCombinedWithNonUnique"}, Check.IsOne,
                    new Option<string>("hint"))
                .Where("nonUnique > 0")
                .HasUniqueness("halfUniqueCombinedWithNonUnique", Check.IsOne).Where("nonUnique > 0")
                .HasUniqueness("halfUniqueCombinedWithNonUnique", Check.IsOne, new Option<string>("hint"))
                .Where("nonUnique > 0");

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDFWithUniqueColumns(_session), check1, new Check[] { });

            CheckResult result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Success);
            IEnumerable<ConstraintStatus> constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            // Half of nonUnique column are duplicates
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            // Half of the 2 columns are duplicates as well.
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);
            // Both next 2 cases are actually unique so should meet threshold
            constraintStatuses.Skip(3).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(4).First().ShouldBe(ConstraintStatus.Success);
            // Nulls are duplicated so this will not be unique
            constraintStatuses.Skip(5).First().ShouldBe(ConstraintStatus.Success);
            // Multi-column uniqueness, duplicates filtered out
            constraintStatuses.Skip(6).First().ShouldBe(ConstraintStatus.Success);
            // Multi-column uniqueness with hint, duplicates filtered out
            constraintStatuses.Skip(7).First().ShouldBe(ConstraintStatus.Success);
            // Single-column uniqueness, duplicates filtered out
            constraintStatuses.Skip(8).First().ShouldBe(ConstraintStatus.Success);
            // Single-column uniqueness with hint, duplicates filtered out
            constraintStatuses.Skip(9).First().ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void should_return_the_correct_status_for_hasUniqueValueRatio()
        {
            Check check1 = new Check(CheckLevel.Error, "unique-value-ratio-check")
                .HasUniqueValueRatio(new[] {"nonUnique", "halfUniqueCombinedWithNonUnique"}, _ => _ == .75,
                    Option<string>.None)
                .HasUniqueValueRatio(new[] {"nonUnique", "halfUniqueCombinedWithNonUnique"}, Check.IsOne,
                    Option<string>.None)
                .Where("nonUnique > 0")
                .HasUniqueValueRatio(new[] {"nonUnique"}, Check.IsOne, new Option<string>("hint"))
                .Where("nonUnique > 0");

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDFWithUniqueColumns(_session), check1, new Check[] { });

            CheckResult result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Success);
            IEnumerable<ConstraintStatus> constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void should_return_the_correct_status_for_histogram_constraints()
        {
            DataFrame df = FixtureSupport.GetDfCompleteAndInCompleteColumns(_session);

            Check check1 = new Check(CheckLevel.Error, "group-1")
                .HasNumberOfDistinctValues("att1", _ => _ < 10, Option<Func<Column, Column>>.None, Option<string>.None)
                .HasHistogramValues("att1", _ => _["a"].Absolute == 4, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att1", _ => _["b"].Absolute == 2, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att1", _ => _["a"].Ratio > .6, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att1", _ => _["b"].Ratio < .4, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att1", _ => _["a"].Absolute == 3, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .Where("att2 is not null")
                .HasHistogramValues("att1", _ => _["b"].Absolute == 1, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .Where("att2 is not null");

            CheckWithLastConstraintFilterable check2 = new Check(CheckLevel.Error, "group-1")
                .HasNumberOfDistinctValues("att2", _ => _ == 3, Option<Func<Column, Column>>.None, Option<string>.None)
                .HasNumberOfDistinctValues("att2", _ => _ == 2, Option<Func<Column, Column>>.None, Option<string>.None)
                .Where("att1 = 'a'")
                .HasHistogramValues("att2", _ => _["f"].Absolute == 3, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att2", _ => _["d"].Absolute == 1, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att2", _ => _[Histogram.NullFieldReplacement].Absolute == 2,
                    Option<Func<Column, Column>>.None, Option<string>.None)
                .HasHistogramValues("att2", _ => _["f"].Ratio == 3 / 6.0, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att2", _ => _["d"].Ratio == 1 / 6.0, Option<Func<Column, Column>>.None,
                    Option<string>.None)
                .HasHistogramValues("att2", _ => _[Histogram.NullFieldReplacement].Ratio == 2 / 6.0,
                    Option<Func<Column, Column>>.None, Option<string>.None);

            CheckWithLastConstraintFilterable check3 = new Check(CheckLevel.Error, "group-1")
                .HasNumberOfDistinctValues("unknownColumn", _ => _ == 3,
                    Option<Func<Column, Column>>.None, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check1, new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Success);
            AssertEvaluatesTo(check3, context, CheckStatus.Error);
        }

        [Fact]
        public void should_return_the_correct_status_for_primary_key()
        {
            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "primary-key-check")
                .IsPrimaryKey("unique", new string[] { })
                .IsPrimaryKey("halfUniqueCombinedWithNonUnique", new[] {"onlyUniqueWithOtherNonUnique"})
                .IsPrimaryKey("halfUniqueCombinedWithNonUnique", new string[] { }).Where("nonUnique > 0")
                .IsPrimaryKey("nonUnique", new Option<string>("hint"), new[] {"halfUniqueCombinedWithNonUnique"})
                .Where("nonUnique > 0 ")
                .IsPrimaryKey("nonUnique", new string[] { })
                .IsPrimaryKey("nonUnique", new[] {"nonUniqueWithNulls"});

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDFWithUniqueColumns(_session), check1, new Check[] { });

            CheckResult result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Error);
            IEnumerable<ConstraintStatus> constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(3).First().ShouldBe(ConstraintStatus.Success);

            constraintStatuses.Skip(4).First().ShouldBe(ConstraintStatus.Failure);
            constraintStatuses.Skip(5).First().ShouldBe(ConstraintStatus.Failure);
        }

        [Fact]
        public void should_return_the_correct_status_for_size()
        {
            DataFrame df = FixtureSupport.GetDfCompleteAndInCompleteColumns(_session);
            long numberOfRows = df.Count();

            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "group-1-S-1")
                .HasSize(_ => _ == numberOfRows, Option<string>.None);
            CheckWithLastConstraintFilterable check2 = new Check(CheckLevel.Warning, "group-1-S-2")
                .HasSize(_ => _ == numberOfRows, Option<string>.None);
            CheckWithLastConstraintFilterable check3 = new Check(CheckLevel.Error, "group-1-E")
                .HasSize(_ => _ != numberOfRows, Option<string>.None);
            CheckWithLastConstraintFilterable check4 = new Check(CheckLevel.Warning, "group-1-W")
                .HasSize(_ => _ != numberOfRows, Option<string>.None);
            CheckWithLastConstraintFilterable check5 = new Check(CheckLevel.Warning, "group-1-W-Range")
                .HasSize(size => size > 0 && size < numberOfRows + 1, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, check1, new Check[] {check2, check3, check4, check5});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Success);
            AssertEvaluatesTo(check3, context, CheckStatus.Error);
            AssertEvaluatesTo(check4, context, CheckStatus.Warning);
            AssertEvaluatesTo(check5, context, CheckStatus.Success);
        }

        [Fact]
        public void should_return_the_correct_status_for_uniqueness()
        {
            CheckWithLastConstraintFilterable check1 = new Check(CheckLevel.Error, "group-1")
                .IsUnique("unique", Option<string>.None)
                .IsUnique("uniqueWithNulls", Option<string>.None)
                .IsUnique("halfUniqueCombinedWithNonUnique", Option<string>.None).Where("nonUnique > 0 ")
                .IsUnique("nonUnique", Option<string>.None)
                .IsUnique("nonUniqueWithNulls", Option<string>.None);

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDFWithUniqueColumns(_session), check1, new Check[] { });

            CheckResult result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Error);
            IEnumerable<ConstraintStatus> constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);

            constraintStatuses.Skip(3).First().ShouldBe(ConstraintStatus.Failure);
            constraintStatuses.Skip(4).First().ShouldBe(ConstraintStatus.Failure);
        }

        [Fact]
        public void should_work_on_regular_expression_patterns_for_E_Mails()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow> {new GenericRow(new object[] {"someone@somewhere.org"})};

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable hasPattern = new Check(CheckLevel.Error, "some description")
                .HasPattern(col, Patterns.Email, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, hasPattern, new Check[] { });

            AssertEvaluatesTo(hasPattern, context, CheckStatus.Success);
        }

        [Fact]
        public void should_work_on_regular_expression_patterns_for_URLs()
        {
            string col = "some";
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"http://foo.com/blah_blah"})
            };

            StructType schema = new StructType(
                new List<StructField> {new StructField(col, new StringType())});

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable hasPattern = new Check(CheckLevel.Error, "some description")
                .HasPattern(col, Patterns.Url, Option<string>.None);

            AnalyzerContext context =
                RunChecks(df, hasPattern, new Check[] { });

            AssertEvaluatesTo(hasPattern, context, CheckStatus.Success);
        }

        [Fact]
        public void should_work_on_regular_expression_patterns_with_filtering()
        {
            string col = "some";

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"someone@somewhere.org", "valid"}),
                new GenericRow(new object[] {"someone@somewhere", "invalid"})
            };

            StructType schema = new StructType(
                new List<StructField>
                {
                    new StructField(col, new StringType()), new StructField("type", new StringType())
                });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            CheckWithLastConstraintFilterable hasPattern =
                new Check(CheckLevel.Error, "some description")
                    .HasPattern(col, Patterns.Email, _ => _ == .5, Option<string>.None);

            Check hasPatternWithFilter =
                new Check(CheckLevel.Error, "some description")
                    .HasPattern(col, Patterns.Email, _ => _ == 1, Option<string>.None)
                    .Where("type = 'valid'");

            AnalyzerContext context =
                RunChecks(df, hasPattern, new[] {hasPatternWithFilter});

            AssertEvaluatesTo(hasPattern, context, CheckStatus.Success);
            AssertEvaluatesTo(hasPatternWithFilter, context, CheckStatus.Success);
        }

        [Fact]
        public void should_yield_correct_results_for_minimum_and_maximum_length_stats()
        {
            CheckWithLastConstraintFilterable hasMin = new Check(CheckLevel.Error, "a")
                .HasMinLength("att1", _ => _ == 0.0, Option<string>.None);

            CheckWithLastConstraintFilterable hasMax = new Check(CheckLevel.Error, "a")
                .HasMaxLength("att1", _ => _ == 4.0, Option<string>.None);

            AnalyzerContext context =
                RunChecks(FixtureSupport.GetDfWithVariableStringLengthValues(_session), hasMin, new[] {hasMax});

            AssertEvaluatesTo(hasMin, context, CheckStatus.Success);
            AssertEvaluatesTo(hasMax, context, CheckStatus.Success);
        }
    }
}
