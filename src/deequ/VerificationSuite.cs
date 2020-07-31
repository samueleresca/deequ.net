using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Applicability;
using xdeequ.Analyzers.Runners;
using xdeequ.Checks;
using xdeequ.Metrics;
using xdeequ.Repository;
using xdeequ.Util;
[assembly: InternalsVisibleTo("deequ.tests")]
namespace xdeequ
{
    internal class VerificationMetricsRepositoryOptions
    {
        public bool failIfResultsForReusingMissing;
        public Option<IMetricsRepository> metricRepository;
        public Option<ResultKey> reuseExistingResultsForKey;
        public Option<ResultKey> saveOrAppendResultsWithKey;

        public VerificationMetricsRepositoryOptions(
            Option<IMetricsRepository> metricRepository = default,
            Option<ResultKey> reuseExistingResultsForKey = default,
            bool failIfResultsForReusingMissing = false,
            Option<ResultKey> saveOrAppendResultsWithKey = default)
        {
            this.metricRepository = metricRepository;
            this.reuseExistingResultsForKey = reuseExistingResultsForKey;
            this.failIfResultsForReusingMissing = failIfResultsForReusingMissing;
            this.saveOrAppendResultsWithKey = saveOrAppendResultsWithKey;
        }
    }

    internal class VerificationFileOutputOptions
    {
        public bool FailIfResultsForReusingMissing;
        public Option<string> SaveCheckResultsJsonToPath;
        public Option<string> SaveSuccessMetricsJsonToPath;
        public Option<SparkSession> SparkSession;

        public VerificationFileOutputOptions(Option<SparkSession> sparkSession,
            Option<string> saveCheckResultsJsonToPath, bool failIfResultsForReusingMissing,
            Option<string> saveSuccessMetricsJsonToPath)
        {
            SparkSession = sparkSession;
            SaveCheckResultsJsonToPath = saveCheckResultsJsonToPath;
            FailIfResultsForReusingMissing = failIfResultsForReusingMissing;
            SaveSuccessMetricsJsonToPath = saveSuccessMetricsJsonToPath;
        }
    }
    /// <summary>
    /// Responsible for running checks and required analysis and return the results
    /// </summary>
    public class VerificationSuite
    {
        /// <summary>
        /// Starting point to construct a VerificationRun.
        /// </summary>
        /// <param name="data">tabular data on which the checks should be verified</param>
        /// <returns></returns>
        public VerificationRunBuilder OnData(DataFrame data) => new VerificationRunBuilder(data);

        /// <summary>
        /// Runs all check groups and returns the verification result. Verification result includes all the metrics computed during the run.
        /// </summary>
        /// <param name="data">tabular data on which the checks should be verified.</param>
        /// <param name="checks">A sequence of check objects to be executed.</param>
        /// <param name="requiredAnalyzers">can be used to enforce the calculation of some some metrics. Regardless of if there are constraints on them (optional)</param>
        /// <param name="aggregateWith">loader from which we retrieve initial states to aggregate (optional)</param>
        /// <param name="saveStatesWith">persist resulting states for the configured analyzers (optional)</param>
        /// <param name="metricsRepositoryOptions">Options related to the MetricsRepository</param>
        /// <param name="fileOutputOptions">Options related to FileOuput using a SparkSession</param>
        /// <returns> Result for every check including the overall status, detailed status for each constraints and all metrics produced</returns>
        internal VerificationResult DoVerificationRun(
            DataFrame data,
            IEnumerable<Check> checks,
            IEnumerable<IAnalyzer<IMetric>> requiredAnalyzers,
            Option<IStateLoader> aggregateWith = default,
            Option<IStatePersister> saveStatesWith = default,
            VerificationMetricsRepositoryOptions metricsRepositoryOptions = null,
            VerificationFileOutputOptions fileOutputOptions = null)
        {
            IEnumerable<IAnalyzer<IMetric>> analyzers =
                requiredAnalyzers.Concat(checks.SelectMany(x => x.RequiredAnalyzers()));

            AnalysisRunnerRepositoryOptions options = new AnalysisRunnerRepositoryOptions(
                metricsRepositoryOptions.metricRepository,
                metricsRepositoryOptions.reuseExistingResultsForKey,
                metricsRepositoryOptions.saveOrAppendResultsWithKey);

            AnalyzerContext analysisResults = AnalysisRunner.DoAnalysisRun(
                data,
                analyzers,
                aggregateWith,
                saveStatesWith,
                new StorageLevel(),
                options);

            VerificationResult verificationResult = Evaluate(checks, analysisResults);

            AnalyzerContext analyzerContext = new AnalyzerContext(verificationResult.Metrics);

            SaveOrAppendResultsIfNecessary(analyzerContext, metricsRepositoryOptions.metricRepository,
                metricsRepositoryOptions.saveOrAppendResultsWithKey);

            SaveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, verificationResult);

            return verificationResult;
        }

        private void SaveJsonOutputsToFilesystemIfNecessary(
            VerificationFileOutputOptions fileOutputOptions,
            VerificationResult verificationResult)
        {
        }


        private void SaveOrAppendResultsIfNecessary(
            AnalyzerContext resultingAnalyzerContext,
            Option<IMetricsRepository> metricsRepository,
            Option<ResultKey> saveOrAppendResultsWithKey) =>
            metricsRepository.Select<object>(repository =>
            {
                saveOrAppendResultsWithKey.Select<object>(key =>
                {
                    Option<AnalyzerContext> currentValueForKey = repository.LoadByKey(key);
                    AnalyzerContext alreadySavedResult =
                            currentValueForKey.GetOrElse(AnalyzerContext.Empty()); // TODO missing override

                    Dictionary<string, IAnalyzer<IMetric>> alreadySavedDict =
                        alreadySavedResult.MetricMap.ToDictionary(pair => pair.Key.ToString(), pair => pair.Key);

                    resultingAnalyzerContext.MetricMap.ToList().ForEach(_ =>
                    {
                        if (alreadySavedDict.ContainsKey(_.Key.ToString()) &&
                            alreadySavedResult.MetricMap.ContainsKey(alreadySavedDict[_.Key.ToString()]))
                        {
                            alreadySavedResult.MetricMap[alreadySavedDict[_.Key.ToString()]] = _.Value;
                        }
                        else
                        {
                            alreadySavedResult.MetricMap.Add(_.Key, _.Value);
                        }
                    });

                    repository.Save(saveOrAppendResultsWithKey.Value, alreadySavedResult);
                    return null;
                });

                return null;
            });

        /// <summary>
        /// Runs all check groups and returns the verification result. Metrics are computed from aggregated states. Verification result includes all the metrics generated during the run
        /// </summary>
        /// <param name="schema">schema of the tabular data on which the checks should be verified</param>
        /// <param name="checks">A sequence of check objects to be executed</param>
        /// <param name="stateLoaders">loaders from which we retrieve the states to aggregate</param>
        /// <param name="requiredAnalysis">can be used to enforce the some metrics regardless of if there are constraints on them (optional)</param>
        /// <param name="saveStatesWith">persist resulting states for the configured analyzers (optional)</param>
        /// <param name="metricsRepository"></param>
        /// <param name="saveOrAppendResultsWithKey"></param>
        /// <returns>Result for every check including the overall status, detailed status for each constraints and all metrics produced</returns>
        public VerificationResult RunOnAggregatedStates(
            StructType schema,
            IEnumerable<Check> checks,
            IEnumerable<IStateLoader> stateLoaders,
            Analysis requiredAnalysis,
            Option<IStatePersister> saveStatesWith,
            Option<IMetricsRepository> metricsRepository,
            Option<ResultKey> saveOrAppendResultsWithKey)
        {
            Analysis analysis = requiredAnalysis.AddAnalyzers(checks.SelectMany(x => x.RequiredAnalyzers()));

            AnalyzerContext analysisResults = AnalysisRunner.RunOnAggregatedStates(
                schema,
                analysis,
                stateLoaders,
                saveStatesWith,
                metricsRepository, saveOrAppendResultsWithKey, new StorageLevel());

            return Evaluate(checks, analysisResults);
        }


        private CheckApplicability IsCheckApplicableToData(Check check, StructType schema, SparkSession sparkSession) =>
            new Applicability(sparkSession).IsApplicable(check, schema);

        private AnalyzersApplicability AreCheckApplicableToData(IEnumerable<IAnalyzer<IMetric>> analyzers,
            StructType schema, SparkSession sparkSession) =>
            new Applicability(sparkSession).IsApplicable(analyzers, schema);


        private VerificationResult Evaluate(IEnumerable<Check> checks, AnalyzerContext analyzerContext)
        {
            IEnumerable<KeyValuePair<Check, CheckResult>> checkResult = checks.Select(check =>
                new KeyValuePair<Check, CheckResult>(check, check.Evaluate(analyzerContext)));

            CheckStatus verificationStatus;

            verificationStatus = !checkResult.Any() ? CheckStatus.Success : checkResult.Max(x => x.Value.Status);

            return new VerificationResult(verificationStatus,
                new Dictionary<Check, CheckResult>(checkResult), analyzerContext.MetricMap);
        }
    }
}
