using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
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
            AnalysisRunnerRepositoryOptions metricsRepositoryOptions = default(AnalysisRunnerRepositoryOptions),
            AnalysisRunnerFileOutputOptions fileOutputOptions = default(AnalysisRunnerFileOutputOptions))

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

            var groupingAnalyzers = passedAnalyzers.Where(x => x is IGroupAnalyzer<IState, IMetric>);

            IEnumerable<IScanSharableAnalyzer<IState, IMetric>> allScanningAnalyzers = passedAnalyzers.Except(groupingAnalyzers).Select(x => (IScanSharableAnalyzer<IState, IMetric>)x);

            var nonGroupedMetrics = RunScanningAnalyzers(data, allScanningAnalyzers, aggregateWith, saveStatesWith);


            Option<double> numRowsOfData = nonGroupedMetrics.Metric(Initializers.Size(Option<string>.None)).Select(x =>
            {
                if (x is DoubleMetric dm)
                    return dm.Value.Success.Value;

                return 0;
            });

            var groupedMetrics = AnalyzerContext.Empty();

            var sortedAndFilteredGroupingAnalyzers = groupingAnalyzers
                .Select(x => (IGroupAnalyzer<IState, IMetric>)x)
                .GroupBy(x => (x.GroupingColumns().OrderBy(x => x), GetFilterCondition(x)));

            foreach (var analyzerGroup in sortedAndFilteredGroupingAnalyzers)
            {
                var (numRows, metrics) =
                    RunGroupingAnalyzers(data,
                        analyzerGroup.Key.Item1.ToList(),
                        analyzerGroup.Key.Item2, analyzerGroup, aggregateWith, saveStatesWith,
                        storageLevelOfGroupedDataForMultiplePasses, numRowsOfData);

                groupedMetrics += metrics;

                if (!numRowsOfData.HasValue)
                    numRowsOfData = new Option<double>(numRows);
            }

            var resultingAnalyzerContext =
                resultComputedPreviously + preconditionFailures + nonGroupedMetrics + groupedMetrics; //TODO: add kllMetrics

            SaveOrAppendResultsIfNecessary(resultingAnalyzerContext,
                metricsRepositoryOptions.metricRepository,
                metricsRepositoryOptions.saveOrAppendResultsWithKey);

            SaveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, resultingAnalyzerContext);

            return resultingAnalyzerContext;
        }


        private static void SaveOrAppendResultsIfNecessary(
            AnalyzerContext resultingAnalyzerContext,
            Option<IMetricsRepository> metricsRepository,
            Option<ResultKey> saveOrAppendResultsWithKey)
        {
            metricsRepository.OnSuccess(repository =>
            {
                saveOrAppendResultsWithKey.OnSuccess(key =>
                 {
                     var currentValueForKey = repository.LoadByKey(key).GetOrElse(AnalyzerContext.Empty());
                     var valueToSave = currentValueForKey + resultingAnalyzerContext;
                     repository.Save(saveOrAppendResultsWithKey.Value, valueToSave);
                 });
            });
        }


        private static void SaveJsonOutputsToFilesystemIfNecessary(
            AnalysisRunnerFileOutputOptions fileOutputOptions,
            AnalyzerContext analyzerContext)
        {
            //TODO implement this part
            return;
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

        private static Option<string> GetFilterCondition(IAnalyzer<IMetric> analyzer)
        {
            if (analyzer is IFilterableAnalyzer fa)
                return fa.FilterCondition();

            return Option<string>.None;
        }


        private static AnalyzerContext RunScanningAnalyzers(DataFrame dataFrame,
            IEnumerable<IScanSharableAnalyzer<IState, IMetric>> analyzers,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateTo
        )
        {
            var sharable = analyzers.Where(x => x is IScanSharableAnalyzer<IState, IMetric>);
            var others = analyzers.Except(sharable);
            var shareableAnalyzers = sharable.Select(_ => (IScanSharableAnalyzer<IState, IMetric>)_);

            if (!shareableAnalyzers.Any())
            {
                return AnalyzerContext.Empty();
            }

            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> metricsByAnalyzer;

            try
            {
                var aggregations = shareableAnalyzers
                    .SelectMany(x => x.AggregationFunctions());

                int i = 0;
                var offsets = shareableAnalyzers.Select(analyzer =>
                {
                    var result = i + analyzer.AggregationFunctions().Count();
                    i++;
                    return result;
                });

                var results = dataFrame.Agg(aggregations.FirstOrDefault(), aggregations.Skip(1).ToArray()).Collect()
                    .First();

                metricsByAnalyzer = shareableAnalyzers
                     .Zip(offsets, (analyzer, i1) => (analyzer, i1))
                     .Select(analyzerOffset => new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzerOffset.analyzer, SuccessOfFailureMetricFrom(analyzerOffset.analyzer, results,
                         analyzerOffset.i1, aggregateWith, saveStateTo)));
            }
            catch (Exception e)
            {
                metricsByAnalyzer = shareableAnalyzers.Select(analyzer =>
                   new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer, analyzer.ToFailureMetric(e)));
            }
            Dictionary<IAnalyzer<IMetric>, IMetric> metricsByAnalyzerDict = new Dictionary<IAnalyzer<IMetric>, IMetric>(metricsByAnalyzer);
            AnalyzerContext sharedResults = new AnalyzerContext(metricsByAnalyzerDict);

            Dictionary<IAnalyzer<IMetric>, IMetric> otherMetrics = new Dictionary<IAnalyzer<IMetric>, IMetric>(others.Select(analyzer =>
                new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer,
                    analyzer.Calculate(dataFrame, aggregateWith, saveStateTo))));

            return sharedResults + new AnalyzerContext(otherMetrics);
        }


        private static (long, AnalyzerContext) RunGroupingAnalyzers(
            DataFrame dataFrame,
            IEnumerable<string> groupingColumns,
            Option<string> filterConditions,
            IEnumerable<IGroupAnalyzer<IState, IMetric>> analyzers,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateTo,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses,
            Option<double> numRowsOfData
        )
        {
            FrequenciesAndNumRows frequenciesAndNumRows =
                FrequencyBasedAnalyzer.ComputeFrequencies(dataFrame, groupingColumns, filterConditions);

            var sampleAnalyzer = analyzers.First() as Analyzer<FrequenciesAndNumRows, IMetric>;

            var previousFrequenciesAndNumRows = aggregateWith.Value.Load<FrequenciesAndNumRows, IMetric>(sampleAnalyzer);

            if (previousFrequenciesAndNumRows.HasValue)
                frequenciesAndNumRows = (FrequenciesAndNumRows)frequenciesAndNumRows.Sum(previousFrequenciesAndNumRows.Value);


            var results = RunAnalyzersForParticularGrouping(frequenciesAndNumRows, analyzers, saveStateTo,
                storageLevelOfGroupedDataForMultiplePasses);

            return (frequenciesAndNumRows.NumRows, results);
        }

        private static AnalyzerContext RunAnalyzersForParticularGrouping(
            FrequenciesAndNumRows frequenciesAndNumRows,
            IEnumerable<IGroupAnalyzer<IState, IMetric>> analyzers,
            Option<IStatePersister> saveStatesTo,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses
        )
        {
            var numRows = frequenciesAndNumRows.NumRows;

            var shareable = analyzers
                .Where(x => x is ScanShareableFrequencyBasedAnalyzer);

            IEnumerable<IGroupAnalyzer<IState, IMetric>> others = analyzers.Except(shareable);

            if (!others.Any())
                frequenciesAndNumRows.Frequencies.Persist(); // TODO: storageLevelOfGroupedDataForMultiplePasses

            var sharableAnalyzers = shareable.Select(x => (ScanShareableFrequencyBasedAnalyzer)x);


            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> metricsByAnalyzer;

            if (!sharableAnalyzers.Any())
                metricsByAnalyzer = new List<KeyValuePair<IAnalyzer<IMetric>, IMetric>>();

            try
            {
                var aggregations = sharableAnalyzers
                    .SelectMany(x => x.AggregationFunctions(numRows));

                int i = 0;
                var offsets = sharableAnalyzers.Select(analyzer =>
                {
                    var result = i + analyzer.AggregationFunctions(numRows).Count();
                    i++;
                    return result;
                });

                var results = frequenciesAndNumRows.Frequencies
                    .Agg(aggregations.FirstOrDefault(), aggregations.Skip(1).ToArray())
                    .Collect()
                    .First();

                metricsByAnalyzer = sharableAnalyzers
                    .Zip(offsets, (analyzer, i1) => (analyzer, i1))
                    .Select(analyzerOffset => new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzerOffset.analyzer,
                        SuccessOfFailureMetricFrom(analyzerOffset.analyzer, results, analyzerOffset.i1)));
            }
            catch (Exception e)
            {
                metricsByAnalyzer = sharableAnalyzers.Select(x =>
                    new KeyValuePair<IAnalyzer<IMetric>, IMetric>(x, x.ToFailureMetric(e)));
            }

            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> otherMetrics;
            try
            {

                otherMetrics = others
                    .Select(x => (FrequencyBasedAnalyzer)x)
                    .Select(x => new KeyValuePair<IAnalyzer<IMetric>, IMetric>(x,
                        x.ComputeMetricFrom(new Option<FrequenciesAndNumRows>(frequenciesAndNumRows))));

            }
            catch (Exception e)
            {
                otherMetrics = others.Select(analyzer =>
                   new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer, analyzer.ToFailureMetric(e)));
            }

            saveStatesTo.Select(_ => _.Persist<FrequenciesAndNumRows, IMetric>((Analyzer<FrequenciesAndNumRows, IMetric>)analyzers.First(), frequenciesAndNumRows));
            frequenciesAndNumRows.Frequencies.Unpersist();

            ;
            return new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(metricsByAnalyzer.Concat(otherMetrics)));
        }




        private static IMetric SuccessOfFailureMetricFrom(
            ScanShareableFrequencyBasedAnalyzer analyzer,
            Row aggregationResult,
            int offset
        )
        {
            try
            {
                return analyzer.FromAggregationResult(aggregationResult, offset);
            }
            catch (Exception e)
            {
                return analyzer.ToFailureMetric(e);
            }
        }


        private static IMetric SuccessOfFailureMetricFrom(
            IScanSharableAnalyzer<IState, IMetric> analyzer,
            Row aggregationResult,
            int offset,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateTo
        )
        {
            try
            {
                return analyzer.MetricFromAggregationResult(aggregationResult, offset, aggregateWith, saveStateTo);
            }
            catch (Exception e)
            {
                return analyzer.ToFailureMetric(e);
            }
        }


    }
    public class AnalysisRunnerRepositoryOptions
    {

        public Option<IMetricsRepository> metricRepository = Option<IMetricsRepository>.None;
        public Option<ResultKey> reuseExistingResultsForKey = Option<ResultKey>.None;
        public Option<ResultKey> saveOrAppendResultsWithKey = Option<ResultKey>.None;
        public bool failIfResultsForReusingMissing = false;

        public AnalysisRunnerRepositoryOptions()
        {

        }

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
        public Option<SparkSession> sparkSession = Option<SparkSession>.None;
        public Option<string> saveSuccessMetricsJsonToPath = Option<string>.None;
        public Option<string> overwriteOutputFiles = Option<string>.None;

        public AnalysisRunnerFileOutputOptions()
        {

        }
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