using Microsoft.Spark.Sql;
using xdeequ.Analyzers.States;
using xdeequ.Constraints;
using Functions = xdeequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    public static class ConstraintUtils
    {
        public static ConstraintResult<S, M, V> Calculate<S, M, V>(IConstraint<S, M, V> constraint, DataFrame dataFrame)
            where S : State<S>
        {
            var analysisBasedConstraint = constraint;
            if (analysisBasedConstraint is ConstraintDecorator<S, M, V>)
            {
                var cd = (ConstraintDecorator<S, M, V>)constraint;
                analysisBasedConstraint = cd.Inner;
            }

            var constraintCasted = (AnalysisBasedConstraint<S, M, V>)analysisBasedConstraint;
            return constraintCasted.CalculateAndEvaluate(dataFrame);
        }
    }
}