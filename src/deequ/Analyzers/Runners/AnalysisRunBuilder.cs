using System.Collections.Generic;
using System.Linq;
using deequ.Metrics;
using deequ.Repository;
using deequ.Util;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers.Runners
{
    internal class AnalysisRunBuilder
    {
        protected IEnumerable<IAnalyzer<IMetric>> _analyzers = new List<IAnalyzer<IMetric>>();
        protected DataFrame data;
        protected bool FailIfResultsForReusingMissing;
        protected Option<IMetricsRepository> metricRepository;
        protected bool overwriteOutputFiles;
        protected Option<ResultKey> ReuseExistingResultsKey;
        protected Option<ResultKey> SaveOrAppendResultsKey;
        protected Option<string> SaveSuccessMetricJsonPath;
        protected Option<SparkSession> sparkSession;


        public AnalysisRunBuilder(DataFrame data) => this.data = data;

        public AnalysisRunBuilder()
        {
        }

        public AnalysisRunBuilder(AnalysisRunBuilder analysisRunBuilder)
        {
            data = analysisRunBuilder.data;
            _analyzers = analysisRunBuilder._analyzers;
            metricRepository = analysisRunBuilder.metricRepository;
            ReuseExistingResultsKey = analysisRunBuilder.ReuseExistingResultsKey;
            FailIfResultsForReusingMissing = analysisRunBuilder.FailIfResultsForReusingMissing;
            SaveOrAppendResultsKey = analysisRunBuilder.SaveOrAppendResultsKey;
            sparkSession = analysisRunBuilder.sparkSession;
            SaveSuccessMetricJsonPath = analysisRunBuilder.SaveSuccessMetricJsonPath;
            overwriteOutputFiles = analysisRunBuilder.overwriteOutputFiles;
        }


        public AnalysisRunBuilder OnData(DataFrame data)
        {
            this.data = data;
            return this;
        }

        public AnalysisRunBuilder AddAnalyzer(IAnalyzer<IMetric> analyzer)
        {
            _analyzers = _analyzers.Append(analyzer);
            return this;
        }

        public AnalysisRunBuilder AddAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {
            _analyzers = _analyzers.Concat(analyzers);
            return this;
        }

        public AnalysisRunBuilderWithRepository UseRepository(IMetricsRepository metricRepository) =>
            new AnalysisRunBuilderWithRepository(this,
                new Option<IMetricsRepository>(metricRepository));


        public AnalysisRunBuilderWithSparkSession UseSparkSession(SparkSession sparkSession) =>
            new AnalysisRunBuilderWithSparkSession(this,
                new Option<SparkSession>(sparkSession));

        public AnalyzerContext Run() =>
            AnalysisRunner.DoAnalysisRun
            (
                data,
                _analyzers,
                Option<IStateLoader>.None,
                Option<IStatePersister>.None,
                new StorageLevel(),
                new AnalysisRunnerRepositoryOptions(
                    metricRepository,
                    ReuseExistingResultsKey,
                    SaveOrAppendResultsKey,
                    FailIfResultsForReusingMissing
                ),
                new AnalysisRunnerFileOutputOptions(
                    sparkSession,
                    SaveSuccessMetricJsonPath,
                    overwriteOutputFiles
                ));
    }

    internal class AnalysisRunBuilderWithRepository : AnalysisRunBuilder
    {
        public AnalysisRunBuilderWithRepository(AnalysisRunBuilder analysisRunBuilder,
            Option<IMetricsRepository> usingMetricRepository) : base(analysisRunBuilder) =>
            metricRepository = usingMetricRepository;

        public AnalysisRunBuilderWithRepository ReuseExistingResultsForKey(ResultKey resultKey,
            bool failIfResultsMissing = false)
        {
            ReuseExistingResultsKey = new Option<ResultKey>(resultKey);
            FailIfResultsForReusingMissing = failIfResultsMissing;
            return this;
        }

        public AnalysisRunBuilderWithRepository SaveOrAppendResult(ResultKey resultKey)
        {
            SaveOrAppendResultsKey = new Option<ResultKey>(resultKey);
            return this;
        }
    }

    internal class AnalysisRunBuilderWithSparkSession : AnalysisRunBuilder
    {
        public AnalysisRunBuilderWithSparkSession(AnalysisRunBuilder analysisRunBuilder,
            Option<SparkSession> usingSparkSession) : base(analysisRunBuilder) =>
            sparkSession = usingSparkSession;

        public AnalysisRunBuilderWithSparkSession SaveSuccessMetricsJsonToPath(string path)
        {
            SaveSuccessMetricJsonPath = new Option<string>(path);
            return this;
        }

        public AnalysisRunBuilderWithSparkSession OverwritePreviousFiles(bool overwriteFiles)
        {
            overwriteOutputFiles = overwriteFiles;
            return this;
        }
    }
}
