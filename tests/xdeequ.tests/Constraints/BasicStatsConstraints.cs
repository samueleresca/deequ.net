using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Constraints;
using xdeequ.Util;
using Xunit;
using static xdeequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    [Collection("Spark instance")]
    public class BasicStatsConstraints
    {
        public BasicStatsConstraints(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact(Skip = "TODO: Implement approximate quantile")]
        public void assert_on_approximate_quantile()
        {
        }

        [Fact]
        public void assert_on_maximum()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            ConstraintResult result = ConstraintUtils.Calculate<MaxState, double, double>(
                MaxConstraint("att1", _ => _ == 6.0, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void assert_on_mean()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            ConstraintResult result = ConstraintUtils.Calculate<MeanState, double, double>(
                MeanConstraint("att1", _ => _ == 3.5, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void assert_on_minimum()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            ConstraintResult result = ConstraintUtils.Calculate<MinState, double, double>(
                MinConstraint("att1", _ => _ == 1.0, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void assert_on_sum()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            ConstraintResult result = ConstraintUtils.Calculate<SumState, double, double>(
                SumConstraint("att1", _ => _ == 21, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }
    }
}
