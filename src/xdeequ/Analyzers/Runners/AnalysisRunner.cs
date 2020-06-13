using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers.Runners
{
    public static class AnalysisRunner
    {

        public static AnalyzerContext DoAnalysisRun
        (
            DataFrame data,
            IEnumerable<IAnalyzer<IMetric>> analyzers,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStatesWith,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses,
            AnalysisRunnerRepositoryOptions metricsRepositoryOptions,
            AnalysisRunnerFileOutputOptions fileOutputOptions)

        {
            if (!analyzers.Any())
            {
                return AnalyzerContext.Empty();
            }

            var allAnalyzers = analyzers.Select(x => (IAnalyzer<IMetric>)x);
            var enumerable = allAnalyzers as IAnalyzer<IMetric>[] ?? allAnalyzers.ToArray();
            var distinctAnalyzers = enumerable.Distinct();


            var resultComputedPreviously = (metricsRepositoryOptions.metricRepository.HasValue,
                    metricsRepositoryOptions.reuseExistingResultsForKey.HasValue) switch
            {
                (true, true) => metricsRepositoryOptions.metricRepository.Value
                    .LoadByKey(metricsRepositoryOptions.reuseExistingResultsForKey.Value).GetOrElse(AnalyzerContext.Empty()),
                _ => AnalyzerContext.Empty()
            };


            var analyzersAlreadyRan = resultComputedPreviously.MetricMap.Keys.AsEnumerable();
            var analyzersToRun = enumerable.Except(analyzersAlreadyRan);

            var passedAnalyzers = analyzersToRun.Where(analyzer =>
                !FindFirstFailing(data.Schema(), analyzer.Preconditions()).HasValue);

            var failedAnalyzers = analyzersToRun.Except(passedAnalyzers);

            var preconditionFailures = ComputePreconditionFailureMetrics(failedAnalyzers, data.Schema());

            //var groupingAnalyzers = passedAnalyzers.Where(x => (x as GroupingAnalyzer<IState, IMetric>) != null);


            return AnalyzerContext.Empty();
        }


        private static Option<Exception> FindFirstFailing(StructType schema, IEnumerable<Action<StructType>> conditions)
        {
            return conditions
                .Select(condition =>
                {
                    try
                    {
                        condition(schema);
                        return Option<Exception>.None;
                    }
                    catch (Exception e)
                    {
                        return new Option<Exception>(e);
                    }
                })
                .First(x => x.HasValue);
        }

        private static AnalyzerContext ComputePreconditionFailureMetrics(IEnumerable<IAnalyzer<IMetric>> failedAnalyzers, StructType schema)
        {
            var failures = failedAnalyzers.Select(analyzer =>
            {
                var firstException = FindFirstFailing(schema, analyzer.Preconditions()).Value;
                return new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer,
                    analyzer.ToFailureMetric(firstException));
            });

            return new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(failures));
        }


    }
    public class AnalysisRunnerRepositoryOptions
    {

        public Option<IMetricsRepository> metricRepository;
        public Option<ResultKey> reuseExistingResultsForKey;
        public Option<ResultKey> saveOrAppendResultsWithKey;
        public bool failIfResultsForReusingMissing;


        public AnalysisRunnerRepositoryOptions(
            Option<IMetricsRepository> metricRepository,
            Option<ResultKey> reuseExistingResultsForKey,
            Option<ResultKey> saveOrAppendResultsWithKey,
            bool failIfResultsForReusingMissing = false
        )
        {
            this.metricRepository = metricRepository;
            this.reuseExistingResultsForKey = reuseExistingResultsForKey;
            this.saveOrAppendResultsWithKey = saveOrAppendResultsWithKey;
            this.failIfResultsForReusingMissing = failIfResultsForReusingMissing;
        }
    }

    public class AnalysisRunnerFileOutputOptions
    {
        public Option<SparkSession> sparkSession;
        public Option<string> saveSuccessMetricsJsonToPath;
        public Option<string> overwriteOutputFiles;

        public AnalysisRunnerFileOutputOptions(
            Option<SparkSession> sparkSession,
            Option<string> saveSuccessMetricsJsonToPath,
            Option<string> overwriteOutputFiles
        )
        {
            this.sparkSession = sparkSession;
            this.saveSuccessMetricsJsonToPath = saveSuccessMetricsJsonToPath;
            this.overwriteOutputFiles = overwriteOutputFiles;
        }
    }

    public class ResultKey
    {
    }

    public class StorageLevel
    {
    }
}