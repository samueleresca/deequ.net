using Shouldly;
using xdeequ.Constraints;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Constraints
{
    public class ConstraintTest
    {
        [Fact]
        public void check_size_constraint()
        {
            IConstraint result = Functions.SizeConstraint(isTest => true, new Option<string>(), new Option<string>());
            result.ShouldNotBeNull();
        }
    }
}
