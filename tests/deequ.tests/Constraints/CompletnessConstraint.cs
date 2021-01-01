using deequ.Constraints;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using Functions = deequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    [Collection("Spark instance")]
    public class CompletenessConstraint
    {
        public CompletenessConstraint(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void assert_on_wrong_completeness()
        {
            DataFrame df = FixtureSupport.GetDFMissing(_session);

            ConstraintUtils.Calculate<double, double>(Functions.CompletenessConstraint("att1",
                d => d == 0.5,
                Option<string>.None, Option<string>.None), df).Status.ShouldBe(ConstraintStatus.Success);

            ConstraintUtils.Calculate<double, double>(Functions.CompletenessConstraint("att1",
                d => d != 0.5,
                Option<string>.None, Option<string>.None), df).Status.ShouldBe(ConstraintStatus.Failure);

            ConstraintUtils.Calculate<double, double>(Functions.CompletenessConstraint("att2",
                d => d == 0.75,
                Option<string>.None, Option<string>.None), df).Status.ShouldBe(ConstraintStatus.Success);

            ConstraintUtils.Calculate<double, double>(Functions.CompletenessConstraint("att2",
                d => d != 0.75,
                Option<string>.None, Option<string>.None), df).Status.ShouldBe(ConstraintStatus.Failure);
        }
    }
}
