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


            var context =
                RunChecks(FixtureSupport.GetDfCompleteAndInCompleteColumns(_session), check1, new Check[] { });

            context.MetricMap.ShouldBeEmpty();
        }

        public static AnalyzerContext RunChecks(DataFrame data, Check check, Check[] checks)
        {
            var analyzers = check.RequiredAnalyzers()
                .Concat(checks.SelectMany(x => x.RequiredAnalyzers())).AsEnumerable();
            return new Analysis(analyzers).Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None, new StorageLevel());
        }
    }
}