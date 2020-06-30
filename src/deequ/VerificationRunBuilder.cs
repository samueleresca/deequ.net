using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.AnomalyDetection;
using xdeequ.Checks;
using xdeequ.Metrics;
using xdeequ.Repository;
using xdeequ.Util;

namespace xdeequ
{
    public class VerificationRunBuilder
    {
        public IEnumerable<Check> checks;
        public DataFrame data;
        public bool failIfResultsForReusingMissing ;
        public Option<IMetricsRepository> metricsRepository;
        public bool overwriteOutputFiles;
        public IEnumerable<IAnalyzer<IMetric>> requiredAnalyzers;
        public Option<ResultKey> reuseExistingResultsKey;
        public Option<string> saveCheckResultsJsonPath;
        public Option<ResultKey> saveOrAppendResultsKey;
        public Option<string> saveSuccessMetricsJsonPath;

        public Option<SparkSession> sparkSession;
        public Option<IStateLoader> stateLoader;

        public Option<IStatePersister> statePersister;


        public VerificationRunBuilder(DataFrame dataFrame)
        {
            data = dataFrame;
            checks = Enumerable.Empty<Check>();
            requiredAnalyzers = Enumerable.Empty<IAnalyzer<IMetric>>();
        }

        public VerificationRunBuilder(VerificationRunBuilder builder)
        {
            requiredAnalyzers = builder.requiredAnalyzers;
            checks = builder.checks;
            metricsRepository = builder.metricsRepository;
            reuseExistingResultsKey = builder.reuseExistingResultsKey;
            failIfResultsForReusingMissing = builder.failIfResultsForReusingMissing;
            saveOrAppendResultsKey = builder.saveOrAppendResultsKey;
            sparkSession = builder.sparkSession;
            overwriteOutputFiles = builder.overwriteOutputFiles;
            saveCheckResultsJsonPath = builder.saveCheckResultsJsonPath;
            saveSuccessMetricsJsonPath = builder.saveSuccessMetricsJsonPath;
            stateLoader = builder.stateLoader;
            statePersister = builder.statePersister;
        }

        /// <summary>
        /// </summary>
        /// <param name="check"></param>
        /// <returns></returns>
        public VerificationRunBuilder AddCheck(Check check)
        {
            checks = checks.Append(check);
            return this;
        }

        public VerificationRunBuilder AddChecks(IEnumerable<Check> checks)
        {
            this.checks.Concat(checks);
            return this;
        }

        public VerificationRunBuilder SaveStateWith(IStatePersister statePersister)
        {
            this.statePersister = new Option<IStatePersister>(statePersister);
            return this;
        }

        public VerificationRunBuilder AggregateWith(IStateLoader stateLoader)
        {
            this.stateLoader = new Option<IStateLoader>(stateLoader);
            return this;
        }

        public VerificationRunBuilder AddRequiredAnalyzer(IAnalyzer<IMetric> requiredAnalyzer)
        {
            requiredAnalyzers = requiredAnalyzers.Append(requiredAnalyzer);
            return this;
        }

        public VerificationRunBuilder AddRequiredAnalyzer(IEnumerable<IAnalyzer<IMetric>> requiredAnalyzers)
        {
            this.requiredAnalyzers = this.requiredAnalyzers.Concat(requiredAnalyzers);
            return this;
        }

        public VerificationRunBuilderWithRepository UseRepository(IMetricsRepository metricsRepository) =>
            new VerificationRunBuilderWithRepository(this,
                new Option<IMetricsRepository>(metricsRepository));

        public VerificationRunBuilderWithSparkSession UseSparkSession(SparkSession sparkSession) =>
            new VerificationRunBuilderWithSparkSession(this, sparkSession);

        public VerificationResult Run() =>
            new VerificationSuite().DoVerificationRun
            (data,
                checks,
                requiredAnalyzers,
                stateLoader,
                statePersister,
                new VerificationMetricsRepositoryOptions(
                    metricsRepository,
                    reuseExistingResultsKey,
                    failIfResultsForReusingMissing,
                    saveOrAppendResultsKey),
                new VerificationFileOutputOptions(
                    sparkSession,
                    saveCheckResultsJsonPath,
                    overwriteOutputFiles,
                    saveSuccessMetricsJsonPath
                )
            );
    }

    public class VerificationRunBuilderWithSparkSession : VerificationRunBuilder
    {
        public VerificationRunBuilderWithSparkSession(VerificationRunBuilder verificationRunBuilder,
            Option<SparkSession> usingSparkSession) : base(verificationRunBuilder) =>
            sparkSession = usingSparkSession;

        public VerificationRunBuilderWithSparkSession SaveCheckResultsJsonToPath(string path)
        {
            saveCheckResultsJsonPath = new Option<string>(path);
            return this;
        }

        public VerificationRunBuilderWithSparkSession SaveSuccessMetricsJsonToPath(string path)
        {
            saveSuccessMetricsJsonPath = new Option<string>(path);
            return this;
        }

        public VerificationRunBuilderWithSparkSession OverwritePreviousFiles(bool overwriteFiles)
        {
            overwriteOutputFiles = overwriteFiles;
            return this;
        }
    }

    public class VerificationRunBuilderWithRepository : VerificationRunBuilder
    {
        public VerificationRunBuilderWithRepository(VerificationRunBuilder verificationRunBuilder,
            Option<IMetricsRepository> usingMetricsRepository) : base(verificationRunBuilder) =>
            metricsRepository = usingMetricsRepository;

        public VerificationRunBuilderWithRepository ReuseExistingResultsForKey(ResultKey resultKey,
            bool failIfResultsMissing = false)
        {
            reuseExistingResultsKey = new Option<ResultKey>(resultKey);
            failIfResultsForReusingMissing = failIfResultsMissing;

            return this;
        }

        public VerificationRunBuilderWithRepository SaveOrAppendResult(ResultKey resultKey)
        {
            saveOrAppendResultsKey = new Option<ResultKey>(resultKey);
            return this;
        }

        public VerificationRunBuilderWithRepository AddAnomalyCheck(
            IAnomalyDetectionStrategy anomalyDetectionStrategy,
            IAnalyzer<IMetric> analyzer,
            Option<AnomalyCheckConfig> anomalyCheckConfig)
        {
            string checkDescription = $"Anomaly check for {analyzer}";
            AnomalyCheckConfig defaultConfig = new AnomalyCheckConfig(CheckLevel.Warning, checkDescription);
            AnomalyCheckConfig anomalyCheckConfigOrDefault = anomalyCheckConfig.GetOrElse(defaultConfig);


            checks = checks.Append(
                new Check(anomalyCheckConfig.Value.Level, anomalyCheckConfig.Value.Description)
                    .IsNewestPointNonAnomalous(
                        metricsRepository,
                        anomalyDetectionStrategy,
                        analyzer,
                        anomalyCheckConfig.Value.WithTagValues,
                        anomalyCheckConfig.Value.AfterDate,
                        anomalyCheckConfig.Value.BeforeDate
                    ));


            return this;
        }
    }

    public class AnomalyCheckConfig
    {
        public Option<long> AfterDate;
        public Option<long> BeforeDate;
        public string Description;
        public CheckLevel Level;
        public Dictionary<string, string> WithTagValues;

        public AnomalyCheckConfig(CheckLevel level, string description)
        {
            Level = level;
            Description = description;
        }
    }
}
