using System.Collections.Generic;
using deequ.Constraints;

namespace deequ.Checks
{
    /// <summary>
    /// A class representing the result of a check.
    /// </summary>
    public class CheckResult
    {
        /// <summary>
        /// The check related to the check result
        /// </summary>
        public Check Check { get;}

        /// <summary>
        /// The status of the check
        /// </summary>
        public CheckStatus Status { get;}

        /// <summary>
        /// The list of constraint results
        /// </summary>
        public IEnumerable<ConstraintResult> ConstraintResults { get;}

        /// <summary>
        /// Ctor of class <see cref="CheckResult"/>
        /// </summary>
        /// <param name="check">The check related to the check result</param>
        /// <param name="status">The status of the check</param>
        /// <param name="constraintResults">The list of the constraint results</param>
        public CheckResult(Check check, CheckStatus status, IEnumerable<ConstraintResult> constraintResults)
        {
            Check = check;
            Status = status;
            ConstraintResults = constraintResults;
        }
    }
}
