using System;
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

namespace xdeequ
{
    public class VerificationMetricsRepositoryOptions
    {
        public bool failIfResultsForReusingMissing;
        public Option<IMetricsRepository> metricRepository;
        public Option<ResultKey> reuseExistingResultsForKey;
        public Option<ResultKey> saveOrAppendResultsWithKey;

        public VerificationMetricsRepositoryOptions(Option<IMetricsRepository> metricRepository,
            Option<ResultKey> reuseExistingResultsForKey, bool failIfResultsForReusingMissing,
            Option<ResultKey> saveOrAppendResultsWithKey)
        {
            this.metricRepository = metricRepository;
            this.reuseExistingResultsForKey = reuseExistingResultsForKey;
            this.failIfResultsForReusingMissing = failIfResultsForReusingMissing;
            this.saveOrAppendResultsWithKey = saveOrAppendResultsWithKey;
        }
    }

    public class VerificationFileOutputOptions
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

    public class VerificationSuite
    {
        public VerificationRunBuilder OnData(DataFrame data) => new VerificationRunBuilder(data);

        public VerificationResult DoVerificationRun(
            DataFrame data,
            IEnumerable<Check> checks,
            IEnumerable<IAnalyzer<IMetric>> requiredAnalyzers,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStatesWith,
            VerificationMetricsRepositoryOptions metricsRepositoryOptions,
            VerificationFileOutputOptions fileOutputOptions)
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

            SaveOrAppendResultsIfNecessary(
                analyzerContext,
                metricsRepositoryOptions.metricRepository,
                metricsRepositoryOptions.saveOrAppendResultsWithKey);

            SaveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, verificationResult);

            return verificationResult;
        }

        private void SaveJsonOutputsToFilesystemIfNecessary(
            VerificationFileOutputOptions fileOutputOptions,
            VerificationResult verificationResult)
        {
            return;
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
                    AnalyzerContext
                        valueToSave = currentValueForKey.GetOrElse(resultingAnalyzerContext); // TODO missing override

                    repository.Save(saveOrAppendResultsWithKey.Value, valueToSave);
                    return null;
                });

                return null;
            });

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

            if (!checkResult.Any())
            {
                verificationStatus = CheckStatus.Success;
            }
            else
            {
                verificationStatus = checkResult.Max(x => x.Value.Status);
            }

            return new VerificationResult(verificationStatus,
                new Dictionary<Check, CheckResult>(checkResult), analyzerContext.MetricMap);
        }
    }
}
