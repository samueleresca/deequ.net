using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Checks;
using deequ.Constraints;

namespace deequ.Extensions
{
    public static class VerificationResultExt
    {
        private const string HEADER =
            "\n\n     _                         _   _ ______ _______ \n    | |                    " +
            "   | \\ | |  ____|__   __|\n  __| | ___  ___  __ _ _   _  |  \\| | |__ " +
            "    | |   \n / _` |/ _ \\/ _ \\/ _` | | |" +
            " | | . ` |  __|    | |   \n| (_| |  __/  __/ (_| | |_| |_| |\\  | |____   | " +
            "|   \n \\__,_|\\___|\\___|\\__, |\\__,_(_)_| \\_|______| " +
            " |_|   \n                    | |                             \n   " +
            "                 |_|                             \n\n\n";

        internal static void Debug(this VerificationResult verificationResult, Action<string> printFunc)
        {
            printFunc(HEADER);
            if (verificationResult.Status == CheckStatus.Success) {
                printFunc("Success");
            } else {
                printFunc("Errors:");
                IEnumerable<ConstraintResult> constraints = verificationResult
                    .CheckResults
                    .SelectMany(pair => pair.Value.ConstraintResults)
                    .Where(c=> c.Status == ConstraintStatus.Failure);

                constraints
                    .Select(constraintResult => $"{constraintResult.Metric.Value.Name} " +
                                                $"of field {constraintResult.Metric.Value.Instance} has the following error: '{constraintResult.Message.GetOrElse(string.Empty)}'")
                    .ToList().ForEach(printFunc);
            }
        }

        public static void Debug(this VerificationResult verificationResult)
        {
            verificationResult.Debug(Console.WriteLine);
        }
    }
}
