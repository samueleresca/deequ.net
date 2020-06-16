using System.Linq;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Checks;
using xdeequ.Constraints;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Checks
{
    [Collection("Spark instance")]
    public class CheckTests
    {
        public CheckTests(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        private readonly SparkSession _session;

        public static AnalyzerContext RunChecks(DataFrame data, Check check, Check[] checks)
        {
            var analyzers = check.RequiredAnalyzers()
                .Concat(checks.SelectMany(x => x.RequiredAnalyzers())).AsEnumerable();
            return new Analysis(analyzers).Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None,
                new StorageLevel());
        }

        public static void AssertEvaluatesTo(Check check, AnalyzerContext analyzerContext, CheckStatus checkStatus)
        {
            check.Evaluate(analyzerContext).Status.ShouldBe(checkStatus);
        }

        [Fact]
        public void Check_should_return_the_correct_check_status_for_any_completeness()
        {
            var check1 = new Check(CheckLevel.Error, "group-1")
                .AreAnyComplete(new[] {"item", "att1"}, Option<string>.None)
                .HaveAnyCompleteness(new[] {"item", "att1"}, _ => _ == 1.0, Option<string>.None);

            var check2 = new Check(CheckLevel.Error, "group-2-E")
                .HaveAnyCompleteness(new[] {"att1", "att2"}, _ => _ > 0.917, Option<string>.None);

            var check3 = new Check(CheckLevel.Warning, "group-2-W")
                .HaveAnyCompleteness(new[] {"att1", "att2"}, _ => _ > 0.917, Option<string>.None);


            var context =
                RunChecks(FixtureSupport.GetDFMissing(_session), check1, new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Warning);
        }

        [Fact]
        public void Check_should_return_the_correct_check_status_for_combined_completeness()
        {
            var check1 = new Check(CheckLevel.Error, "group-1")
                .AreComplete(new[] {"item", "att1"}, Option<string>.None)
                .HaveCompleteness(new[] {"item", "att1"}, _ => _ == 1.0, Option<string>.None);

            var check2 = new Check(CheckLevel.Error, "group-2-E")
                .HaveCompleteness(new[] {"item", "att1", "att2"}, _ => _ > 0.8, Option<string>.None);

            var check3 = new Check(CheckLevel.Warning, "group-2-W")
                .HaveCompleteness(new[] {"item", "att1", "att2"}, _ => _ > 0.8, Option<string>.None);


            var context =
                RunChecks(FixtureSupport.GetDfCompleteAndInCompleteColumns(_session), check1,
                    new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Warning);
        }

        [Fact]
        public void Check_should_return_the_correct_check_status_for_completness()
        {
            var check1 = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None)
                .HasCompleteness("att1", _ => _ == 1.0, Option<string>.None);

            var check2 = new Check(CheckLevel.Error, "group-2-E")
                .HasCompleteness("att2", _ => _ > 0.8, Option<string>.None);

            var check3 = new Check(CheckLevel.Warning, "group-2-W")
                .HasCompleteness("att2", _ => _ > 0.8, Option<string>.None);


            var context =
                RunChecks(FixtureSupport.GetDfCompleteAndInCompleteColumns(_session), check1,
                    new Check[] {check2, check3});

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Warning);
        }

        [Fact]
        public void Check_should_return_the_correct_check_status_for_distinctness()
        {
            var check1 = new Check(CheckLevel.Error, "distinctness-check")
                .HasDistinctness(new[] {"att1"}, _ => _ == 3.0 / 5, Option<string>.None)
                .HasDistinctness(new[] {"att1"}, _ => _ == 2.0 / 3, Option<string>.None).Where("att2 is not null")
                .HasDistinctness(new[] {"att1", "att2"}, _ => _ == 4.0 / 6, Option<string>.None)
                .HasDistinctness(new[] {"att2"}, _ => _ == 1.0, Option<string>.None);

            var context =
                RunChecks(FixtureSupport.GetDfWithDistinctValues(_session), check1, new Check[] { });

            var result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Error);
            var constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(3).First().ShouldBe(ConstraintStatus.Failure);
        }

        [Fact]
        public void Check_should_return_the_correct_check_status_for_primary_key()
        {
            var check1 = new Check(CheckLevel.Error, "primary-key-check")
                .IsPrimaryKey("unique", new string[] { })
                .IsPrimaryKey("halfUniqueCombinedWithNonUnique", new[] {"onlyUniqueWithOtherNonUnique"})
                .IsPrimaryKey("halfUniqueCombinedWithNonUnique", new string[] { }).Where("nonUnique > 0")
                .IsPrimaryKey("nonUnique", new Option<string>("hint"), new[] {"halfUniqueCombinedWithNonUnique"})
                .Where("nonUnique > 0 ")
                .IsPrimaryKey("nonUnique", new string[] { })
                .IsPrimaryKey("nonUnique", new[] {"nonUniqueWithNulls"});

            var context =
                RunChecks(FixtureSupport.GetDFWithUniqueColumns(_session), check1, new Check[] { });

            var result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Error);
            var constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(3).First().ShouldBe(ConstraintStatus.Success);

            constraintStatuses.Skip(4).First().ShouldBe(ConstraintStatus.Failure);
            constraintStatuses.Skip(5).First().ShouldBe(ConstraintStatus.Failure);
        }

        [Fact]
        public void Check_should_return_the_correct_check_status_for_uniqueness()
        {
            var check1 = new Check(CheckLevel.Error, "group-1")
                .IsUnique("unique", Option<string>.None)
                .IsUnique("uniqueWithNulls", Option<string>.None)
                .IsUnique("halfUniqueCombinedWithNonUnique", Option<string>.None).Where("nonUnique > 0 ")
                .IsUnique("nonUnique", Option<string>.None)
                .IsUnique("nonUniqueWithNulls", Option<string>.None);

            var context =
                RunChecks(FixtureSupport.GetDFWithUniqueColumns(_session), check1, new Check[] { });

            var result = check1.Evaluate(context);
            result.Status.ShouldBe(CheckStatus.Error);
            var constraintStatuses = result.ConstraintResults.Select(x => x.Status);
            constraintStatuses.First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(1).First().ShouldBe(ConstraintStatus.Success);
            constraintStatuses.Skip(2).First().ShouldBe(ConstraintStatus.Success);

            constraintStatuses.Skip(3).First().ShouldBe(ConstraintStatus.Failure);
            constraintStatuses.Skip(4).First().ShouldBe(ConstraintStatus.Failure);
        }
    }
}