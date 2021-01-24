using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers;
using deequ.Constraints;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.tests.Analyzers;
using Xunit;

namespace xdeequ.tests.Constraints
{

    [Collection("Spark instance")]
    public class AnalysisBasedConstraintTest
    {
        public AnalysisBasedConstraintTest(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact(Skip = "Check concurrent execution")]

        public void assert_correctly_on_values_if_analysis_is_successful()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);

            ConstraintResult resultA = ConstraintUtils.Calculate<double, double>(
                new AnalysisBasedConstraint<double, double>(
                    new Completeness("att1"), val => val == 1.0, Option<string>.None), df);

            resultA.Status.ShouldBe(ConstraintStatus.Success);
            resultA.Message.ShouldBe(Option<string>.None);
            resultA.Metric.HasValue.ShouldBeTrue();


            ConstraintResult resultB = ConstraintUtils.Calculate<double, double>(
                new AnalysisBasedConstraint<double, double>(
                    new Completeness("att1"), val => val != 1.0, Option<string>.None), df);

            resultB.Status.ShouldBe(ConstraintStatus.Failure);
            resultB.Message.ShouldBe("Value: 1 does not meet the constraint requirement!");
            resultB.Metric.HasValue.ShouldBeTrue();


            ConstraintResult resultC = ConstraintUtils.Calculate<double, double>(
                new AnalysisBasedConstraint<double, double>(
                    new Completeness("someMissingColumn"), val => val == 1.0, Option<string>.None), df);

            resultC.Status.ShouldBe(ConstraintStatus.Failure);
            resultC.Message.ShouldBe("Input data does not include column someMissingColumn!");
            resultC.Metric.HasValue.ShouldBeTrue();
        }

        [Fact]
        public void execute_value_picker_on_the_analysis_result_value_if_provided()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);

            ConstraintResult resultA = ConstraintUtils.Calculate<double, double>(
                new AnalysisBasedConstraint<double, double>(
                    new Completeness("att1"), val => val == 2.0, new Option<Func<double, double>>(value => value * 2),
                    Option<string>.None), df);

            resultA.Status.ShouldBe(ConstraintStatus.Success);

            ConstraintResult resultB = ConstraintUtils.Calculate<double, double>(
                new AnalysisBasedConstraint<double, double>(
                    new Completeness("att1"), val => val != 2.0, new Option<Func<double, double>>(value => value * 2),
                    Option<string>.None), df);

            resultB.Status.ShouldBe(ConstraintStatus.Failure);

            ConstraintResult resultC = ConstraintUtils.Calculate<double, double>(
                new AnalysisBasedConstraint<double, double>(
                    new Completeness("someMissingColumn"), val => val != 2.0,
                    new Option<Func<double, double>>(value => value * 2), Option<string>.None), df);

            resultC.Status.ShouldBe(ConstraintStatus.Failure);
        }


        [Fact]
        public void execute_value_picker_on_the_analysis_result_value_retrieved_from_context_if_provided()
        {
            Completeness att1Analyzer = new Completeness("att1");

            DataFrame df = FixtureSupport.GetDFFull(_session);
            Dictionary<string, JvmObjectReference> validResults = new Dictionary<string, JvmObjectReference>
            {
                {att1Analyzer.ToString(), new Completeness("att1").Calculate(df).Reference}
            };

            new AnalysisBasedConstraint<double, double>(att1Analyzer, val => val == 2.0,
                    new Option<Func<double, double>>(val => val * 2), Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Success);
        }


        [Fact]
        public void fail_on_analysis_if_value_picker_is_provided_but_fails()
        {
            Func<double, double> problematicValuePicker = d => throw new Exception("failed");
            Completeness att1Analyzer = new Completeness("att1");
            DataFrame df = FixtureSupport.GetDFFull(_session);

            Dictionary<string, JvmObjectReference> emptyResult = new Dictionary<string, JvmObjectReference>();
            Dictionary<string, JvmObjectReference> validResults = new Dictionary<string, JvmObjectReference>
            {
                {att1Analyzer.ToString(), new Completeness("att1").Calculate(df).Reference}
            };

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(att1Analyzer, val => val == 2.0,
                    new Option<Func<double, double>>(problematicValuePicker), Option<string>.None);

            ConstraintResult result = ConstraintUtils.Calculate<double, double>(constraint, df);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.Value.Contains("Can't retrieve the value to assert on").ShouldBeTrue();
            result.Metric.HasValue.ShouldBeTrue();

            ConstraintResult validConstraint = constraint.Evaluate(validResults);
            validConstraint.Status.ShouldBe(ConstraintStatus.Failure);
            validConstraint.Message.Value.Contains("Can't retrieve the value to assert on").ShouldBeTrue();
            validConstraint.Metric.HasValue.ShouldBeTrue();


            ConstraintResult emptyResults = constraint.Evaluate(emptyResult);
            emptyResults.Status.ShouldBe(ConstraintStatus.Failure);
            emptyResults.Message.Value.Contains("Missing Analysis, can't run the constraint!").ShouldBeTrue();
            emptyResults.Metric.HasValue.ShouldBeFalse();
        }


        [Fact(Skip = "Check concurrent execution")]
        public void fail_on_failed_assertion_function_with_hint_in_exception_message_if_provided()
        {
            Completeness att1Analyzer = new Completeness("att1");
            DataFrame df = FixtureSupport.GetDFFull(_session);

            AnalysisBasedConstraint<double, double> failingConstraint =
                new AnalysisBasedConstraint<double, double>(att1Analyzer, val => val == 0.9,
                    new Option<string>("Value should be like ...!"));

            ConstraintResult result = ConstraintUtils.Calculate<double, double>(failingConstraint, df);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.Value.ShouldBe("Value: 1 does not meet the constraint requirement!" +
                                          "Value should be like ...!");
            result.Metric.HasValue.ShouldBeTrue();
        }

        [Fact]
        public void get_the_analysis_from_the_context_if_provided()
        {
            Completeness att1Analyzer = new Completeness("att1");
            Completeness someMissingColumn = new Completeness("someMissingColumn");

            DataFrame df = FixtureSupport.GetDFMissing(_session);
            Dictionary<string, JvmObjectReference> emptyResult = new Dictionary<string, JvmObjectReference>();
            Dictionary<string, JvmObjectReference> validResults = new Dictionary<string, JvmObjectReference>
            {
                {att1Analyzer.ToString(), new Completeness("att1").Calculate(df).Reference},
                {someMissingColumn.ToString(), new Completeness("someMissingColumn").Calculate(df).Reference}
            };

            new AnalysisBasedConstraint<double, double>(att1Analyzer, val => val == 1.0, Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Success);

            new AnalysisBasedConstraint<double, double>(att1Analyzer, val => val != 1.0, Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Failure);

            new AnalysisBasedConstraint<double, double>(someMissingColumn, val => val != 1.0,
                    Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Failure);


            ConstraintResult result = new AnalysisBasedConstraint<double, double>(att1Analyzer,
                    val => val == 1.0,
                    Option<string>.None)
                .Evaluate(emptyResult);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.ShouldBe("Missing Analysis, can't run the constraint!");
            result.Metric.HasValue.ShouldBeFalse();
        }

        [Fact]
        public void return_failed_constraint_for_a_failing_assertion()
        {
            string msg = "-test-";
            Exception exception = new Exception(msg);
            DataFrame df = FixtureSupport.GetDFFull(_session);
            Func<double, bool> failingAssertion = d => throw exception;

            AnalysisBasedConstraint<double, double> failingConstraint =
                new AnalysisBasedConstraint<double, double>(new Completeness("att1"),
                    failingAssertion, Option<string>.None);
            ConstraintResult result = ConstraintUtils.Calculate<double, double>(failingConstraint, df);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.Value.ShouldBe($"Can't execute the assertion: {msg}");
            result.Metric.HasValue.ShouldBeTrue();
        }
    }
}
