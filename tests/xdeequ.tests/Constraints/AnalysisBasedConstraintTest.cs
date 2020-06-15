using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Constraints;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Constraints
{
    class SampleAnalyzer : Analyzer<NumMatches, DoubleMetric>, IAnalyzer<DoubleMetric>
    {
        public readonly string _column;

        public SampleAnalyzer(string column)
        {
            _column = column;
        }
        public override Option<NumMatches> ComputeStateFrom(DataFrame dataFrame)
        {
            throw new NotImplementedException();
        }

        public override DoubleMetric ComputeMetricFrom(Option<NumMatches> state)
        {
            throw new NotImplementedException();
        }

        public DoubleMetric Calculate(DataFrame data)
        {

            var valueTry = Try<double>.From(() =>
                {
                    if (!data.Columns().Any(x => x == _column))
                        throw new Exception($"requirement failed: Missing column {_column}");

                    return 1.0;
                });

            return new DoubleMetric(Entity.Column, "sample", _column, valueTry);
        }

        public override DoubleMetric ToFailureMetric(Exception e)
        {
            return new DoubleMetric(Entity.Column, "sample", _column, new Try<double>(e));
        }
    }

    [Collection("Spark instance")]
    public class AnalysisBasedConstraintTest
    {
        private readonly SparkSession _session;

        public AnalysisBasedConstraintTest(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        [Fact]
        public void assert_correctly_on_values_if_analysis_is_successful()
        {

            var df = FixtureSupport.GetDFMissing(_session);

            var resultA = ConstraintUtils.Calculate<NumMatches, double, double>(new AnalysisBasedConstraint<NumMatches, double, double>(
                new SampleAnalyzer("att1"), _ => _ == 1.0, Option<string>.None), df);

            resultA.Status.ShouldBe(ConstraintStatus.Success);
            resultA.Message.ShouldBe(Option<string>.None);
            resultA.Metric.ShouldNotBeNull();


            var resultB = ConstraintUtils.Calculate<NumMatches, double, double>(new AnalysisBasedConstraint<NumMatches, double, double>(
                new SampleAnalyzer("att1"), _ => _ != 1.0, Option<string>.None), df);

            resultB.Status.ShouldBe(ConstraintStatus.Failure);
            resultB.Message.ShouldBe("Value: 1 does not meet the constraint requirement!");
            resultB.Metric.HasValue.ShouldBeTrue();


            var resultC = ConstraintUtils.Calculate<NumMatches, double, double>(new AnalysisBasedConstraint<NumMatches, double, double>(
                new SampleAnalyzer("someMissingColumn"), _ => _ == 1.0, Option<string>.None), df);

            resultC.Status.ShouldBe(ConstraintStatus.Failure);
            resultC.Message.ShouldBe("requirement failed: Missing column someMissingColumn");
            resultC.Metric.HasValue.ShouldBeTrue();



        }

        [Fact]
        public void execute_value_picker_on_the_analysis_result_value_if_provided()
        {
            var df = FixtureSupport.GetDFMissing(_session);

            var resultA = ConstraintUtils.Calculate<NumMatches, double, double>(new AnalysisBasedConstraint<NumMatches, double, double>(
                new SampleAnalyzer("att1"), _ => _ == 2.0, new Option<Func<double, double>>(value => value * 2), Option<string>.None), df);

            resultA.Status.ShouldBe(ConstraintStatus.Success);

            var resultB = ConstraintUtils.Calculate<NumMatches, double, double>(new AnalysisBasedConstraint<NumMatches, double, double>(
                new SampleAnalyzer("att1"), _ => _ != 2.0, new Option<Func<double, double>>(value => value * 2), Option<string>.None), df);

            resultB.Status.ShouldBe(ConstraintStatus.Failure);

            var resultC = ConstraintUtils.Calculate<NumMatches, double, double>(new AnalysisBasedConstraint<NumMatches, double, double>(
                new SampleAnalyzer("someMissingColumn"), _ => _ != 2.0, new Option<Func<double, double>>(value => value * 2), Option<string>.None), df);

            resultC.Status.ShouldBe(ConstraintStatus.Failure);
        }

        [Fact]
        public void get_the_analysis_from_the_context_if_provided()
        {
            var att1Analyzer = new SampleAnalyzer("att1");
            var someMissingColumn = new SampleAnalyzer("someMissingColumn");

            var df = FixtureSupport.GetDFMissing(_session);
            var emptyResult = new Dictionary<IAnalyzer<IMetric>, IMetric>();
            var validResults = new Dictionary<IAnalyzer<IMetric>, IMetric>
            {
                {att1Analyzer, new SampleAnalyzer("att1").Calculate(df) },
                {someMissingColumn, new SampleAnalyzer("someMissingColumn").Calculate(df) },
            };

            new AnalysisBasedConstraint<NumMatches, double, double>(att1Analyzer, _ => _ == 1.0, Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Success);

            new AnalysisBasedConstraint<NumMatches, double, double>(att1Analyzer, _ => _ != 1.0, Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Failure);

            new AnalysisBasedConstraint<NumMatches, double, double>(someMissingColumn, _ => _ != 1.0, Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Failure);


            var result = new AnalysisBasedConstraint<NumMatches, double, double>(att1Analyzer, _ => _ == 1.0,
                    Option<string>.None)
                .Evaluate(emptyResult);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.ShouldBe("Missing Analysis, can't run the constraint!");
            result.Metric.HasValue.ShouldBeFalse();
        }


        [Fact]
        public void execute_value_picker_on_the_analysis_result_value_retrieved_from_context_if_provided()
        {
            var att1Analyzer = new SampleAnalyzer("att1");

            var df = FixtureSupport.GetDFMissing(_session);
            var validResults = new Dictionary<IAnalyzer<IMetric>, IMetric>
            {
                {att1Analyzer, new SampleAnalyzer("att1").Calculate(df) }
            };

            new AnalysisBasedConstraint<NumMatches, double, double>(att1Analyzer, _ => _ == 2.0, new Option<Func<double, double>>(_ => _ * 2), Option<string>.None)
                .Evaluate(validResults)
                .Status.ShouldBe(ConstraintStatus.Success);
        }



        [Fact]
        public void fail_on_analysis_if_value_picker_is_provided_but_fails()
        {
            var problematicValuePicker = new Func<double, double>(d => throw new RuntimeWrappedException("failed"));
            var att1Analyzer = new SampleAnalyzer("att1");
            var emptyResult = new Dictionary<IAnalyzer<IMetric>, IMetric>();

            var df = FixtureSupport.GetDFMissing(_session);
            var validResults = new Dictionary<IAnalyzer<IMetric>, IMetric>
            {
                {att1Analyzer, new SampleAnalyzer("att1").Calculate(df) }
            };

            var constraint = new AnalysisBasedConstraint<NumMatches, double, double>(att1Analyzer, _ => _ == 2.0,
                new Option<Func<double, double>>(problematicValuePicker), Option<string>.None);

            var result = ConstraintUtils.Calculate<NumMatches, double, double>(constraint, df);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.Value.Contains("Can't retrieve the value to assert on").ShouldBeTrue();
            result.Metric.HasValue.ShouldBeTrue();


            var validConstraint = constraint.Evaluate(validResults);
            validConstraint.Status.ShouldBe(ConstraintStatus.Failure);
            validConstraint.Message.Value.Contains("Can't retrieve the value to assert on").ShouldBeTrue();
            validConstraint.Metric.HasValue.ShouldBeTrue();


            var emptyResults = constraint.Evaluate(emptyResult);
            emptyResults.Status.ShouldBe(ConstraintStatus.Failure);
            emptyResults.Message.Value.Contains("Missing Analysis, can't run the constraint!").ShouldBeTrue();
            emptyResults.Metric.HasValue.ShouldBeFalse();

        }



        [Fact]
        public void fail_on_failed_assertion_function_with_hint_in_exception_message_if_provided()
        {
            var problematicValuePicker = new Func<double, double>(d => throw new RuntimeWrappedException("failed"));
            var att1Analyzer = new SampleAnalyzer("att1");
            var df = FixtureSupport.GetDFMissing(_session);

            var failingConstraint = new AnalysisBasedConstraint<NumMatches, double, double>(att1Analyzer, _ => _ == 2.0,
                new Option<Func<double, double>>(problematicValuePicker), new Option<string>("Value should be like ...!"));

            var result = ConstraintUtils.Calculate<NumMatches, double, double>(failingConstraint, df);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.Value.ShouldBe("Value: 1.0 does not meet the constraint requirement!" + "Value should be like ...!");
            result.Metric.HasValue.ShouldBeTrue();
        }

        [Fact]
        public void return_failed_constraint_for_a_failing_assertion()
        {
            var msg = "-test-";
            var exception = new RuntimeWrappedException(msg);
            var df = FixtureSupport.GetDFMissing(_session);
            var failingAssertion = new Func<double, bool>(d => throw exception);

            var failingConstraint = new AnalysisBasedConstraint<NumMatches, double, double>(new SampleAnalyzer("att1"), failingAssertion, Option<string>.None);
            var result = ConstraintUtils.Calculate<NumMatches, double, double>(failingConstraint, df);

            result.Status.ShouldBe(ConstraintStatus.Failure);
            result.Message.Value.ShouldBe($"Can't execute the assertion: {msg}");
            result.Metric.HasValue.ShouldBeTrue();
        }
    }
}