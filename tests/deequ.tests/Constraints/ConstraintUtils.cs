using deequ.Constraints;
using Microsoft.Spark.Sql;

namespace xdeequ.tests.Constraints
{
    public static class ConstraintUtils
    {
        public static ConstraintResult Calculate<M, V>(IConstraint constraint, DataFrame dataFrame)
        {
            IConstraint analysisBasedConstraint = constraint;
            if (analysisBasedConstraint is ConstraintDecorator)
            {
                ConstraintDecorator cd = (ConstraintDecorator)constraint;
                analysisBasedConstraint = cd.Inner;
            }

            AnalysisBasedConstraint<M, V> constraintCasted =
                (AnalysisBasedConstraint<M, V>)analysisBasedConstraint;
            return constraintCasted.CalculateAndEvaluate(dataFrame);
        }
    }
}
