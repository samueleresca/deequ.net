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
            AnalysisRunnerRepositoryOptions metricsRepositoryOptions = default,
            AnalysisRunnerFileOutputOptions fileOutputOptions = default)
        {
            if (!analyzers.Any()) return AnalyzerContext.Empty();

            var allAnalyzers = analyzers.Select(x => x);
            var enumerable = allAnalyzers as IAnalyzer<IMetric>[] ?? allAnalyzers.ToArray();
            var distinctAnalyzers = enumerable.Distinct();


            var resultComputedPreviously = (metricsRepositoryOptions?.metricRepository.HasValue,
                    metricsRepositoryOptions?.reuseExistingResultsForKey.HasValue) switch
            {
                (true, true) => metricsRepositoryOptions?.metricRepository.Value
                    .LoadByKey(metricsRepositoryOptions.reuseExistingResultsForKey.Value)
                    .GetOrElse(AnalyzerContext.Empty()),
                _ => AnalyzerContext.Empty()
            };


            var analyzersAlreadyRan = resultComputedPreviously.MetricMap.Keys.AsEnumerable();
            var analyzersToRun = enumerable.Except(analyzersAlreadyRan);

            var passedAnalyzers = analyzersToRun.Where(analyzer =>
                !FindFirstFailing(data.Schema(), analyzer.Preconditions()).HasValue);

            var failedAnalyzers = analyzersToRun.Except(passedAnalyzers);

            var preconditionFailures = ComputePreconditionFailureMetrics(failedAnalyzers, data.Schema());

            var groupingAnalyzers = passedAnalyzers.OfType<IGroupAnalyzer<IState, IMetric>>();

            var allScanningAnalyzers = passedAnalyzers.Except(groupingAnalyzers).Select(x => x);

            var nonGroupedMetrics = RunScanningAnalyzers(data, allScanningAnalyzers, aggregateWith, saveStatesWith);

            var numRowsOfData = nonGroupedMetrics.Metric(Initializers.Size(Option<string>.None)).Select(x =>
            {
                if (x is DoubleMetric dm)
                    return dm.Value.Success.Value;
                return 0;
            });

            var groupedMetrics = AnalyzerContext.Empty();

            var sortedAndFilteredGroupingAnalyzers = groupingAnalyzers
                .Select(x => x)
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
                resultComputedPreviously + preconditionFailures + nonGroupedMetrics +
                groupedMetrics; //TODO: add kllMetrics

            if (metricsRepositoryOptions != null)
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
                .FirstOrDefault(x => x.HasValue);
        }

        private static AnalyzerContext ComputePreconditionFailureMetrics(
            IEnumerable<IAnalyzer<IMetric>> failedAnalyzers, StructType schema)
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
            IEnumerable<IAnalyzer<IMetric>> analyzers,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateTo
        )
        {
            var sharable = analyzers.OfType<IScanSharableAnalyzer<IState, IMetric>>();
            var others = analyzers.Except(sharable);

            AnalyzerContext sharedResults;
            if (sharable.Any())
            {
                IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> metricsByAnalyzer;

                try
                {
                    var aggregations = sharable
                        .SelectMany(x => x.AggregationFunctions());

                    var i = 0;


                    var offsets = sharable.Select(analyzer =>
                    {
                        i += analyzer.AggregationFunctions().Count();
                        return i;
                    }).ToList();

                    offsets.Insert(0, 0);

                    var results = dataFrame.Agg(aggregations.FirstOrDefault(), aggregations.Skip(1).ToArray()).Collect()
                        .First();

                    metricsByAnalyzer = sharable
                        .Zip(offsets, (analyzer, i1) => (analyzer, i1))
                        .Select(analyzerOffset => new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzerOffset.analyzer,
                            SuccessOfFailureMetricFrom(analyzerOffset.analyzer, results,
                                analyzerOffset.i1, aggregateWith, saveStateTo)));
                }
                catch (Exception e)
                {
                    metricsByAnalyzer = sharable.Select(analyzer =>
                        new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer, analyzer.ToFailureMetric(e)));
                }

                var metricsByAnalyzerDict = new Dictionary<IAnalyzer<IMetric>, IMetric>(metricsByAnalyzer);
                sharedResults = new AnalyzerContext(metricsByAnalyzerDict);
            }
            else sharedResults = AnalyzerContext.Empty();

            var otherMetrics = new Dictionary<IAnalyzer<IMetric>, IMetric>(others.Select(analyzer =>
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
            var frequenciesAndNumRows =
                FrequencyBasedAnalyzer.ComputeFrequencies(dataFrame, groupingColumns, filterConditions);

            var sampleAnalyzer = analyzers.First() as Analyzer<FrequenciesAndNumRows, IMetric>;

            var previousFrequenciesAndNumRows = aggregateWith
                .Select(x => x.Load<FrequenciesAndNumRows, IMetric>(sampleAnalyzer))
                .GetOrElse(Option<FrequenciesAndNumRows>.None);

            if (previousFrequenciesAndNumRows.HasValue)
                frequenciesAndNumRows =
                    (FrequenciesAndNumRows)frequenciesAndNumRows.Sum(previousFrequenciesAndNumRows.Value);


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
                .OfType<ScanShareableFrequencyBasedAnalyzer>();

            var others = analyzers.Except(shareable);

            if (!others.Any())
                frequenciesAndNumRows.Frequencies.Persist(); // TODO: storageLevelOfGroupedDataForMultiplePasses

            var sharableAnalyzers = shareable.Select(x => x);


            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> metricsByAnalyzer;

            if (!sharableAnalyzers.Any())
                metricsByAnalyzer = new List<KeyValuePair<IAnalyzer<IMetric>, IMetric>>();
            else
                try
                {
                    var aggregations = sharableAnalyzers
                        .SelectMany(x => x.AggregationFunctions(numRows));

                    var i = 0;
                    var offsets = sharableAnalyzers.Select(analyzer =>
                    {
                        i += analyzer.AggregationFunctions(numRows).Count();
                        return i;
                    }).ToList();

                    offsets.Insert(0, 0);

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

            saveStatesTo.Select(_ =>
                _.Persist<FrequenciesAndNumRows, IMetric>((Analyzer<FrequenciesAndNumRows, IMetric>)analyzers.First(),
                    frequenciesAndNumRows));
            frequenciesAndNumRows.Frequencies.Unpersist();

            ;
            return new AnalyzerContext(
                new Dictionary<IAnalyzer<IMetric>, IMetric>(metricsByAnalyzer.Concat(otherMetrics)));
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
        public bool failIfResultsForReusingMissing;

        public Option<IMetricsRepository> metricRepository = Option<IMetricsRepository>.None;
        public Option<ResultKey> reuseExistingResultsForKey = Option<ResultKey>.None;
        public Option<ResultKey> saveOrAppendResultsWithKey = Option<ResultKey>.None;

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
        public Option<bool> overwriteOutputFiles = Option<bool>.None;
        public Option<string> saveSuccessMetricsJsonToPath = Option<string>.None;
        public Option<SparkSession> sparkSession = Option<SparkSession>.None;

        public AnalysisRunnerFileOutputOptions()
        {
        }

        public AnalysisRunnerFileOutputOptions(
            Option<SparkSession> sparkSession,
            Option<string> saveSuccessMetricsJsonToPath,
            Option<bool> overwriteOutputFiles
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