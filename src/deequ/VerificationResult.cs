using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Checks;
using deequ.Extensions;
using deequ.Metrics;
using Microsoft.Spark.Sql;

namespace deequ
{
    /**
  * The result returned from the VerificationSuite
  *
  * @param status The overall status of the Verification run
  * @param checkResults Checks and their CheckResults
  * @param metrics Analyzers and their Metric results
  */
    public class VerificationResult
    {
        public Dictionary<Check, CheckResult> CheckResults;
        public Dictionary<IAnalyzer<IMetric>, IMetric> Metrics;
        public CheckStatus Status;

        public VerificationResult(CheckStatus status, Dictionary<Check, CheckResult> checkResults,
            Dictionary<IAnalyzer<IMetric>, IMetric> metrics)
        {
            Status = status;
            CheckResults = checkResults;
            Metrics = metrics;
        }

        public VerificationResult(VerificationResult verificationResult)
        {
            CheckResults = verificationResult.CheckResults;
            Metrics = verificationResult.Metrics;
            Status = verificationResult.Status;
        }


        public DataFrame SuccessMetricsAsDataFrame(SparkSession session, IEnumerable<IAnalyzer<IMetric>> forAnalyzers)
        {
            AnalyzerContext metricsAsAnalyzerContext = new AnalyzerContext(Metrics);
            return metricsAsAnalyzerContext.SuccessMetricsAsDataFrame(session, forAnalyzers);
        }

        public string SuccessMetricsAsJson(IEnumerable<IAnalyzer<IMetric>> forAnalyzers)
        {
            AnalyzerContext metricsAsAnalyzerContext = new AnalyzerContext(Metrics);
            return metricsAsAnalyzerContext.SuccessMetricsAsJson(forAnalyzers);
        }

        public DataFrame CheckResultsAsDataFrame()
        {
            IEnumerable<SimpleCheckResultOutput> simplifiedCheckResults = GetSimplifiedCheckResultOutput();
            return simplifiedCheckResults.ToDataFrame();
        }

        public string CheckResultAsJson(VerificationResult verificationResult, IEnumerable<Check> forChecks)
        {
            IEnumerable<SimpleCheckResultOutput> simplifiedCheckResults = GetSimplifiedCheckResultOutput();


            List<Dictionary<string, string>> result = new List<Dictionary<string, string>>();
            foreach (SimpleCheckResultOutput check in simplifiedCheckResults)
            {
                Dictionary<string, string> elements = new Dictionary<string, string>
                {
                    {"check", check.CheckDescription},
                    {"check_level", check.CheckLevel},
                    {"check_status", check.CheckStatus},
                    {"constraint", check.Constraint},
                    {"constraint_status", check.ConstraintStatus},
                    {"constraint_message", check.ConstraintMessage}
                };

                result.Add(elements);
            }

            return JsonSerializer.Serialize(result, SerdeExt.GetDefaultOptions());
        }


        private IEnumerable<SimpleCheckResultOutput> GetSimplifiedCheckResultOutput()
        {
            Dictionary<Check, CheckResult>.ValueCollection selectedCheckResults = CheckResults
                .Values;

            return selectedCheckResults.SelectMany(checkResult =>
            {
                return checkResult.ConstraintResults.Select(constraintResult => new SimpleCheckResultOutput(
                    checkResult.Check.Description,
                    checkResult.Check.Level.ToString(),
                    checkResult.Status.ToString(),
                    constraintResult.Constraint.ToString(),
                    constraintResult.Status.ToString(),
                    constraintResult.Message.GetOrElse(string.Empty)));
            });
        }
    }

    internal class SimpleCheckResultOutput
    {
        public readonly string CheckDescription;
        public readonly string CheckLevel;
        public readonly string CheckStatus;
        public readonly string Constraint;
        public readonly string ConstraintMessage;
        public readonly string ConstraintStatus;

        public SimpleCheckResultOutput(string checkDescription, string checkLevel, string checkStatus,
            string constraint, string constraintStatus, string constraintMessage)
        {
            CheckDescription = checkDescription;
            CheckLevel = checkLevel;
            CheckStatus = checkStatus;
            Constraint = constraint;
            ConstraintStatus = constraintStatus;
            ConstraintMessage = constraintMessage;
        }
    }
}
