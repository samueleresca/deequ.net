using System;
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
    public class MutualInformationConstraints
    {
        private readonly SparkSession _session;

        public MutualInformationConstraints(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        [Fact]
        public void yield_a_mutual_information_of_0_for_conditionally_uninformative_columns()
        {
            var df = FixtureSupport.GetDfWithConditionallyUninformativeColumns(_session);

            ConstraintUtils.Calculate<FrequenciesAndNumRows, double, double>(MutualInformationConstraint("att1", "att2",
                    _ => _ == 0,
                    Option<string>.None, Option<string>.None), df)
                .Status.ShouldBe(ConstraintStatus.Success);
        }
    }
}