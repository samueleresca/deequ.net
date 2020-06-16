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
    public class MinAndMaxConstraints
    {
        public MinAndMaxConstraints(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        private readonly SparkSession _session;

        [Fact]
        public void assert_on_max_length()
        {
            var df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            var result = ConstraintUtils.Calculate<MaxState, double, double>(
                MaxLengthConstraint("att1", _ => _ == 4.0, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void assert_on_min_length()
        {
            var df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            var result = ConstraintUtils.Calculate<MinState, double, double>(
                MinLengthConstraint("att1", _ => _ == 0.0, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }
    }
}