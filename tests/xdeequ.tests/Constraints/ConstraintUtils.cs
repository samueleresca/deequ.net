using Microsoft.Spark.Sql;
using xdeequ.Analyzers.States;
using xdeequ.Constraints;
using Functions = xdeequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    public static class ConstraintUtils
    {
        public static ConstraintResult Calculate<S, M, V>(IConstraint constraint, DataFrame dataFrame)
            where S : State<S>
        {
            var analysisBasedConstraint = constraint;
            if (analysisBasedConstraint is ConstraintDecorator)
            {
                var cd = (ConstraintDecorator)constraint;
                analysisBasedConstraint = cd.Inner;
            }

            var constraintCasted = (AnalysisBasedConstraint<S, M, V>)analysisBasedConstraint;
            return constraintCasted.CalculateAndEvaluate(dataFrame);
        }
    }
}