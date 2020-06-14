using System.Linq;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Checks;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Checks
{
    [Collection("Spark instance")]
    public class CheckTests
    {
        private readonly SparkSession _session;
        public CheckTests(SparkFixture fixture)
        {
            _session = fixture.Spark;
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
                RunChecks(FixtureSupport.GetDfCompleteAndInCompleteColumns(_session), check1, new Check[] { check2, check3 });

            AssertEvaluatesTo(check1, context, CheckStatus.Success);
            AssertEvaluatesTo(check2, context, CheckStatus.Error);
            AssertEvaluatesTo(check3, context, CheckStatus.Warning);
        }

        public static AnalyzerContext RunChecks(DataFrame data, Check check, Check[] checks)
        {
            var analyzers = check.RequiredAnalyzers()
                .Concat(checks.SelectMany(x => x.RequiredAnalyzers())).AsEnumerable();
            return new Analysis(analyzers).Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None, new StorageLevel());
        }

        public static void AssertEvaluatesTo(Check check, AnalyzerContext analyzerContext, CheckStatus checkStatus)
        {
            check.Evaluate(analyzerContext).Status.ShouldBe(checkStatus);
        }
    }
}