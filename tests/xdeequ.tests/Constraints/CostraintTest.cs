using Shouldly;
using Xunit;
using xdeequ.Constraints;
using xdeequ.Util;

namespace xdeequ.tests.Constraints
{
    public class ConstraintTest
    {

        [Fact]
        public void check_size_constraint()
        {
           var result =  Constraint.SizeConstraint((isTest) => true, new Option<string>(), new Option<string>());
            result.ShouldNotBeNull();
        }
    }
}