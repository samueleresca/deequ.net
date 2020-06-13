using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers.Runners;
using xdeequ.Checks;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Checks
{
    public class CheckTests
    {

        [Fact]
        public void Check_should_return_the_correct_check_status_for_completness()
        {
            var check1 = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None)
                .HasCompleteness("att1", _ => _ == 1.0, Option<string>.None);

        }

        public static AnalyzerContext RunChecks(DataFrame data, Check check, Check[] checks)
        {
            var analyzers = check.RequiredAnalyzers()
                .Concat(checks.SelectMany(x => x.RequiredAnalyzers())).AsEnumerable();
            //TODO implement AnalysisRunners

            return AnalyzerContext.Empty();
        }
    }

}