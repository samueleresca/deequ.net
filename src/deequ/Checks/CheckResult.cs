using System.Collections.Generic;
using deequ.Constraints;

namespace deequ.Checks
{
    public class CheckResult
    {
        public CheckResult(Check check, CheckStatus status, IEnumerable<ConstraintResult> constraintResult)
        {
            Check = check;
            Status = status;
            ConstraintResults = constraintResult;
        }

        public Check Check { get; set; }
        public CheckStatus Status { get; set; }
        public IEnumerable<ConstraintResult> ConstraintResults { get; set; }
    }
}
