using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Analyzers.States;
using xdeequ.AnomalyDetection;
using xdeequ.Checks;
using xdeequ.Metrics;
using xdeequ.Repository;
using xdeequ.Util;

namespace xdeequ
{
    /// <summary>
    /// A class to build a Verification run using a fluent API
    /// </summary>
    public class VerificationRunBuilder
    {
        private readonly DataFrame _data;

        protected IEnumerable<Check> checks;
        protected bool failIfResultsForReusingMissing;
        protected Option<IMetricsRepository> metricsRepository;
        protected bool overwriteOutputFiles;
        protected IEnumerable<IAnalyzer<IMetric>> requiredAnalyzers;
        protected Option<ResultKey> reuseExistingResultsKey;
        protected Option<string> saveCheckResultsJsonPath;
        protected Option<ResultKey> saveOrAppendResultsKey;
        protected Option<string> saveSuccessMetricsJsonPath;
        protected Option<SparkSession> sparkSession;

        private Option<IStateLoader> stateLoader;
        private Option<IStatePersister> statePersister;

        /// <summary>
        ///
        /// </summary>
        /// <param name="dataFrame"></param>
        public VerificationRunBuilder(DataFrame dataFrame)
        {
            _data = dataFrame;
            checks = Enumerable.Empty<Check>();
            requiredAnalyzers = Enumerable.Empty<IAnalyzer<IMetric>>();
        }

        public VerificationRunBuilder(VerificationRunBuilder builder)
        {
            _data = builder._data;
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
        /// Add a single check to the run.
        /// </summary>
        /// <param name="check">A check object to be executed during the run</param>
        /// <returns></returns>
        public VerificationRunBuilder AddCheck(Check check)
        {
            checks = checks.Append(check);
            return this;
        }
        /// <summary>
        /// Add multiple checks to the run.
        /// </summary>
        /// <param name="checks">A sequence of check objects to be executed during the run.</param>
        /// <returns></returns>
        public VerificationRunBuilder AddChecks(IEnumerable<Check> checks)
        {
            this.checks = this.checks.Concat(checks);
            return this;
        }

        /// <summary>
        /// Save analyzer states.  Enables aggregate computation of metrics later, e.g., when a new partition is added to the dataset.
        /// </summary>
        /// <param name="statePersister">A state persister that saves the computed states for later aggregation.</param>
        /// <returns></returns>
        public VerificationRunBuilder SaveStateWith(IStatePersister statePersister)
        {
            this.statePersister = new Option<IStatePersister>(statePersister);
            return this;
        }

        /// <summary>
        ///  Use to load saved analyzer states and aggregate them with those calculated in this new run. Can be used to efficiently compute metrics for a large dataset if e.g. a new partition is added.
        /// </summary>
        /// <param name="stateLoader">A state loader that loads previously calculated states and allows aggregation with the ones calculated in this run.</param>
        /// <returns></returns>
        public VerificationRunBuilder AggregateWith(IStateLoader stateLoader)
        {
            this.stateLoader = new Option<IStateLoader>(stateLoader);
            return this;
        }


        /// <summary>
        ///  Can be used to enforce the calculation of some some metric regardless of if there is a constraint on it (optional)
        /// </summary>
        /// <param name="requiredAnalyzer">The analyzers to be used to calculate the metrics during the run.</param>
        /// <returns></returns>
        public VerificationRunBuilder AddRequiredAnalyzer(IAnalyzer<IMetric> requiredAnalyzer)
        {
            requiredAnalyzers = requiredAnalyzers.Append(requiredAnalyzer);
            return this;
        }


        /// <summary>
        ///  Can be used to enforce the calculation of some some metric regardless of if there is a constraint on it (optional)
        /// </summary>
        /// <param name="requiredAnalyzers">The analyzers to be used to calculate the metrics during the run.</param>
        /// <returns></returns>
        public VerificationRunBuilder AddRequiredAnalyzers(IEnumerable<IAnalyzer<IMetric>> requiredAnalyzers)
        {
            this.requiredAnalyzers = this.requiredAnalyzers.Concat(requiredAnalyzers);
            return this;
        }


        /// <summary>
        /// Set a metrics repository associated with the current data to enable features like reusing previously computed results and storing the results of the current run.
        /// </summary>
        /// <param name="metricsRepository">A metrics repository to store and load results associated with the run</param>
        /// <returns></returns>
        public VerificationRunBuilderWithRepository UseRepository(IMetricsRepository metricsRepository) =>
            new VerificationRunBuilderWithRepository(this,
                new Option<IMetricsRepository>(metricsRepository));


        /// <summary>
        /// Use a <see cref="SparkSession"/> to conveniently create output files.
        /// </summary>
        /// <param name="sparkSession">the spark session to use</param>
        /// <returns></returns>
        public VerificationRunBuilderWithSparkSession UseSparkSession(SparkSession sparkSession) =>
            new VerificationRunBuilderWithSparkSession(this, sparkSession);

        /// <summary>
        /// Run the verification suite
        /// </summary>
        /// <returns>Returns the <see cref="VerificationResult"/> resulted by the suite</returns>
        public VerificationResult Run() =>
            new VerificationSuite().DoVerificationRun
            (_data,
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

    /// <summary>
    /// Reuse any previously computed results stored in the metrics repository associated with the current data to save computation time.
    /// </summary>
    public class VerificationRunBuilderWithRepository : VerificationRunBuilder
    {

        /// <summary>
        /// Ctor of class <see cref="VerificationRunBuilderWithRepository"/>>
        /// </summary>
        /// <param name="verificationRunBuilder">The verification run builder base instance</param>
        /// <param name="usingMetricsRepository">The <see cref="IMetricsRepository"/> interface used by the builder</param>
        public VerificationRunBuilderWithRepository(VerificationRunBuilder verificationRunBuilder,
            Option<IMetricsRepository> usingMetricsRepository) : base(verificationRunBuilder) =>
            metricsRepository = usingMetricsRepository;


        /// <summary>
        /// Reuse any previously computed results stored in the metrics repository associated with the current data to save computation time.
        /// </summary>
        /// <param name="resultKey">The exact result key of the previously computed result</param>
        /// <param name="failIfResultsMissing">Whether the run should fail if new metric calculations are needed</param>
        /// <returns></returns>
        public VerificationRunBuilderWithRepository ReuseExistingResultsForKey(ResultKey resultKey,
            bool failIfResultsMissing = false)
        {
            reuseExistingResultsKey = new Option<ResultKey>(resultKey);
            failIfResultsForReusingMissing = failIfResultsMissing;

            return this;
        }


        /// <summary>
        /// A shortcut to save the results of the run or append them to existing results in the metrics repository.
        /// </summary>
        /// <param name="resultKey">The result key to identify the current run.</param>
        /// <returns></returns>
        public VerificationRunBuilderWithRepository SaveOrAppendResult(ResultKey resultKey)
        {
            saveOrAppendResultsKey = new Option<ResultKey>(resultKey);
            return this;
        }

        /// <summary>
        ///  Can be used to enforce the calculation of some some metric regardless of if there is a constraint on it (optional)
        /// </summary>
        /// <param name="requiredAnalyzer">The analyzers to be used to calculate the metrics during the run.</param>
        /// <returns></returns>
        public new VerificationRunBuilderWithRepository AddRequiredAnalyzer(IAnalyzer<IMetric> requiredAnalyzer)
        {
            requiredAnalyzers = requiredAnalyzers.Append(requiredAnalyzer);
            return this;
        }

        /// <summary>
        ///  Can be used to enforce the calculation of some some metric regardless of if there is a constraint on it (optional)
        /// </summary>
        /// <param name="requiredAnalyzers">The analyzers to be used to calculate the metrics during the run.</param>
        /// <returns></returns>
        public new VerificationRunBuilderWithRepository AddRequiredAnalyzers(
            IEnumerable<IAnalyzer<IMetric>> requiredAnalyzers)
        {
            this.requiredAnalyzers = this.requiredAnalyzers.Concat(requiredAnalyzers);
            return this;
        }

        /// <summary>
        /// Add a check using Anomaly Detection methods. The Anomaly Detection Strategy only checks if the new value is an Anomaly.
        /// </summary>
        /// <param name="anomalyDetectionStrategy">The anomaly detection strategy</param>
        /// <param name="analyzer">The analyzer for the metric to run anomaly detection on.</param>
        /// <param name="anomalyCheckConfig">Some configuration settings for the Check.</param>
        /// <returns></returns>
        public VerificationRunBuilderWithRepository AddAnomalyCheck(
            IAnomalyDetectionStrategy anomalyDetectionStrategy,
            IAnalyzer<IMetric> analyzer,
            Option<AnomalyCheckConfig> anomalyCheckConfig)
        {
            string checkDescription = $"Anomaly check for {analyzer}";

            AnomalyCheckConfig defaultConfig = new AnomalyCheckConfig(CheckLevel.Warning, checkDescription);
            AnomalyCheckConfig anomalyCheckConfigOrDefault = anomalyCheckConfig.GetOrElse(defaultConfig);


            checks = checks.Append(
                new Check(anomalyCheckConfigOrDefault.Level, anomalyCheckConfigOrDefault.Description)
                    .IsNewestPointNonAnomalous<IState>(
                        metricsRepository.Value,
                        anomalyDetectionStrategy,
                        analyzer,
                        anomalyCheckConfigOrDefault.WithTagValues,
                        anomalyCheckConfigOrDefault.AfterDate,
                        anomalyCheckConfigOrDefault.BeforeDate
                    ));

            return this;
        }
    }


    /// <summary>
    ///
    /// </summary>
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
            AfterDate = Option<long>.None;
            BeforeDate = Option<long>.None;
            WithTagValues = new Dictionary<string, string>();
        }

        public AnomalyCheckConfig(CheckLevel level, string description, Dictionary<string, string> withTagValues,
            Option<long> afterDate, Option<long> beforeDate)
        {
            Level = level;
            Description = description;
            WithTagValues = withTagValues;
            AfterDate = afterDate;
            BeforeDate = beforeDate;
        }
    }
}
