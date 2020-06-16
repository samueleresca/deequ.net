using Microsoft.Spark.Sql;
using xdeequ.Analyzers.States;
using xdeequ.Constraints;

namespace xdeequ.tests.Constraints
{
    public static class ConstraintUtils
    {
        public static ConstraintResult Calculate<S, M, V>(IConstraint constraint, DataFrame dataFrame)
            where S : IState
        {
            var analysisBasedConstraint = constraint;
            if (analysisBasedConstraint is ConstraintDecorator)
            {
                var cd = (ConstraintDecorator) constraint;
                analysisBasedConstraint = cd.Inner;
            }

            var constraintCasted = (AnalysisBasedConstraint<S, M, V>) analysisBasedConstraint;
            return constraintCasted.CalculateAndEvaluate(dataFrame);
        }
    }
}