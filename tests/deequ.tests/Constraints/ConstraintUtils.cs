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
            IConstraint analysisBasedConstraint = constraint;
            if (analysisBasedConstraint is ConstraintDecorator)
            {
                ConstraintDecorator cd = (ConstraintDecorator)constraint;
                analysisBasedConstraint = cd.Inner;
            }

            AnalysisBasedConstraint<S, M, V> constraintCasted =
                (AnalysisBasedConstraint<S, M, V>)analysisBasedConstraint;
            return constraintCasted.CalculateAndEvaluate(dataFrame);
        }
    }
}
