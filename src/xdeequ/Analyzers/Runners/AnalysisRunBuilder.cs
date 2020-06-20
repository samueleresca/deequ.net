using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers.Runners
{
    public class AnalysisRunBuilder
    {
        protected DataFrame data;
        protected IEnumerable<IAnalyzer<IMetric>> _analyzers;
        protected Option<IMetricsRepository> metricRepository;
        protected Option<ResultKey> ReuseExistingResultsKey;
        protected bool FailIfResultsForReusingMissing = false;
        protected Option<ResultKey> SaveOrAppendResultsKey;
        protected Option<SparkSession> sparkSession;
        protected Option<string> SaveSuccessMetricJsonPath;
        protected bool overwriteOutputFiles = false;
        
        
        public AnalysisRunBuilder(DataFrame data)
        {
            this.data = data;
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


        public AnalysisRunBuilder AddAnalyzer(IAnalyzer<IMetric> analyzer)
        {
            _analyzers = _analyzers.Append(analyzer);
            return this;
        }

        public AnalysisRunBuilder AddAnalyzer(IEnumerable<IAnalyzer<IMetric>> analyzers)
        {
            _analyzers = _analyzers.Concat(analyzers);
            return this;
        }
        
        public AnalysisRunBuilderWithRepository UseRepository(IMetricsRepository metricRepository)
        {
            return new AnalysisRunBuilderWithRepository(this, 
                new Option<IMetricsRepository>(metricRepository));
        }
        
        
        public AnalysisRunBuilderWithSparkSession UseSparkSession(SparkSession sparkSession)
        {
            return new AnalysisRunBuilderWithSparkSession(this, 
                new Option<SparkSession>(sparkSession));
        }

        public AnalyzerContext Run()
        {
           return AnalysisRunner.DoAnalysisRun
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
    }

    public class AnalysisRunBuilderWithRepository : AnalysisRunBuilder
    {
        public AnalysisRunBuilderWithRepository(AnalysisRunBuilder analysisRunBuilder, 
            Option<IMetricsRepository> usingMetricRepository) : base(analysisRunBuilder)
        {
            metricRepository = usingMetricRepository;
        }

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
    
    public class AnalysisRunBuilderWithSparkSession : AnalysisRunBuilder
    {
        public AnalysisRunBuilderWithSparkSession(AnalysisRunBuilder analysisRunBuilder, 
            Option<SparkSession> usingSparkSession) : base(analysisRunBuilder)
        {
            sparkSession = usingSparkSession;
        }

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