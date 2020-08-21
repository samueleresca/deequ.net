using deequ.Analyzers;
using deequ.Constraints;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static deequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    [Collection("Spark instance")]
    public class MinAndMaxConstraints
    {
        public MinAndMaxConstraints(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void assert_on_max_length()
        {
            DataFrame df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            ConstraintResult result = ConstraintUtils.Calculate<MaxState, double, double>(
                MaxLengthConstraint("att1", val => val == 4.0, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void assert_on_min_length()
        {
            DataFrame df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            ConstraintResult result = ConstraintUtils.Calculate<MinState, double, double>(
                MinLengthConstraint("att1", val => val == 0.0, Option<string>.None, Option<string>.None), df);
            result.Status.ShouldBe(ConstraintStatus.Success);
        }
    }
}
