using deequ.Constraints;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static deequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    [Collection("Spark instance")]
    public class MutualInformationConstraints
    {
        public MutualInformationConstraints(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void yield_a_mutual_information_of_0_for_conditionally_uninformative_columns()
        {
            DataFrame df = FixtureSupport.GetDfWithConditionallyUninformativeColumns(_session);

            ConstraintUtils.Calculate<double, double>(MutualInformationConstraint("att1", "att2",
                    val => val == 0,
                    Option<string>.None, Option<string>.None), df)
                .Status.ShouldBe(ConstraintStatus.Success);
        }
    }
}
