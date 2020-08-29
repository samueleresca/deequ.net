using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers.States;
using deequ.Metrics;
using deequ.Repository;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers.Runners
{
    public static class AnalysisRunner
    {

        /// <summary>
        ///
        /// </summary>
        /// <param name="data"></param>
        /// <param name="analyzers"></param>
        /// <param name="aggregateWith"></param>
        /// <param name="saveStatesWith"></param>
        /// <param name="storageLevelOfGroupedDataForMultiplePasses"></param>
        /// <returns></returns>
        public static AnalyzerContext Run(
            DataFrame data,
            Analysis analysis,
            Option<IStateLoader> aggregateWith = default,
            Option<IStatePersister> saveStatesWith = default,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK
            )
        {
            return DoAnalysisRun(data,
                analysis.Analyzers,
                aggregateWith,
                saveStatesWith,
                storageLevelOfGroupedDataForMultiplePasses);
        }


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
            if (!analyzers.Any())
            {
                return AnalyzerContext.Empty();
            }

            IEnumerable<IAnalyzer<IMetric>> allAnalyzers = analyzers.Select(analyzer => analyzer);
            IAnalyzer<IMetric>[] enumerable = allAnalyzers as IAnalyzer<IMetric>[] ?? allAnalyzers.ToArray();
            IEnumerable<IAnalyzer<IMetric>> distinctAnalyzers = enumerable.Distinct();


            AnalyzerContext resultComputedPreviously = (metricsRepositoryOptions?.metricRepository.HasValue,
                    metricsRepositoryOptions?.reuseExistingResultsForKey.HasValue) switch
            {
                (true, true) => metricsRepositoryOptions?.metricRepository.Value
                    .LoadByKey(metricsRepositoryOptions.reuseExistingResultsForKey.Value)
                    .GetOrElse(AnalyzerContext.Empty()),
                _ => AnalyzerContext.Empty()
            };


            IEnumerable<IAnalyzer<IMetric>>
                analyzersAlreadyRan = resultComputedPreviously.MetricMap.Keys.AsEnumerable();
            IEnumerable<IAnalyzer<IMetric>> analyzersToRun = enumerable.Except(analyzersAlreadyRan);

            IEnumerable<IAnalyzer<IMetric>> passedAnalyzers = analyzersToRun.Where(analyzer =>
                !FindFirstFailing(data.Schema(), analyzer.Preconditions()).HasValue);

            IEnumerable<IAnalyzer<IMetric>> failedAnalyzers = analyzersToRun.Except(passedAnalyzers);

            AnalyzerContext preconditionFailures = ComputePreconditionFailureMetrics(failedAnalyzers, data.Schema());

            IEnumerable<IGroupingAnalyzer<IMetric>> groupingAnalyzers =
                passedAnalyzers.OfType<IGroupingAnalyzer<IMetric>>();

            IEnumerable<IAnalyzer<IMetric>> allScanningAnalyzers =
                passedAnalyzers.Except(groupingAnalyzers).Select(analyzer => analyzer);

            AnalyzerContext nonGroupedMetrics =
                RunScanningAnalyzers(data, allScanningAnalyzers, aggregateWith, saveStatesWith);

            Option<double> numRowsOfData = nonGroupedMetrics.Metric(Initializers.Size()).Select(metric =>
            {
                if (metric is DoubleMetric dm)
                {
                    return dm.Value.Success.Value;
                }

                return 0;
            });

            AnalyzerContext groupedMetrics = AnalyzerContext.Empty();

            IEnumerable<IGrouping<(IOrderedEnumerable<string>, Option<string>), IGroupingAnalyzer<IMetric>>>
                sortedAndFilteredGroupingAnalyzers = groupingAnalyzers
                    .Select(analyzer => analyzer)
                    .GroupBy(analyzer => (analyzer.GroupingColumns().OrderBy(columnName => columnName), GetFilterCondition(analyzer)));

            foreach (IGrouping<(IOrderedEnumerable<string>, Option<string>), IGroupingAnalyzer<IMetric>>
                analyzerGroup in sortedAndFilteredGroupingAnalyzers)
            {
                (long numRows, AnalyzerContext metrics) =
                    RunGroupingAnalyzers(data,
                        analyzerGroup.Key.Item1.ToList(),
                        analyzerGroup.Key.Item2, analyzerGroup, aggregateWith, saveStatesWith,
                        storageLevelOfGroupedDataForMultiplePasses, numRowsOfData);

                groupedMetrics += metrics;

                if (!numRowsOfData.HasValue)
                {
                    numRowsOfData = new Option<double>(numRows);
                }
            }

            AnalyzerContext resultingAnalyzerContext =
                resultComputedPreviously + preconditionFailures + nonGroupedMetrics +
                groupedMetrics; //TODO: add kllMetrics

            if (metricsRepositoryOptions != null)
            {
                SaveOrAppendResultsIfNecessary(resultingAnalyzerContext,
                    metricsRepositoryOptions.metricRepository,
                    metricsRepositoryOptions.saveOrAppendResultsWithKey);
            }

            SaveJsonOutputsToFilesystemIfNecessary(fileOutputOptions, resultingAnalyzerContext);

            return resultingAnalyzerContext;
        }

        /**
         * Compute the metrics from the analyzers configured in the analyis, instead of running
         * directly on data, this computation leverages (and aggregates) existing states which have
         * previously been computed on the data.
         *
         * @param schema schema of the data frame from which the states were computed
         * @param analysis the analysis to compute
         * @param stateLoaders loaders from which we retrieve the states to aggregate
         * @param saveStatesWith persist resulting states for the configured analyzers (optional)
         * @param storageLevelOfGroupedDataForMultiplePasses caching level for grouped data that must be
         * accessed multiple times (use
         * StorageLevel.NONE to completely disable
         * caching)
         * @return AnalyzerContext holding the requested metrics per analyzer
         */
        public static AnalyzerContext RunOnAggregatedStates(
            StructType schema,
            Analysis analysis,
            IEnumerable<IStateLoader> stateLoaders,
            Option<IStatePersister> saveStatesWith = default,
            Option<IMetricsRepository> metricsRepository = default,
            Option<ResultKey> saveOrAppendResultsWithKey = default,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses = StorageLevel.MEMORY_AND_DISK)
        {
            if (analysis.Analyzers == null || stateLoaders == null)
            {
                return AnalyzerContext.Empty();
            }

            IEnumerable<IAnalyzer<IMetric>> analyzers = analysis.Analyzers;

            /* Find all analyzers which violate their preconditions */
            IEnumerable<IAnalyzer<IMetric>> passedAnalyzers = analyzers
                .Where(analyzer => !FindFirstFailing(schema, analyzer.Preconditions()).HasValue);

            IEnumerable<IAnalyzer<IMetric>> failedAnalyzers = analyzers.Except(passedAnalyzers);

            /* Create the failure metrics from the precondition violations */
            AnalyzerContext preconditionFailures = ComputePreconditionFailureMetrics(failedAnalyzers, schema);

            InMemoryStateProvider aggregatedStates = new InMemoryStateProvider();

            foreach (IAnalyzer<IMetric> analyzer in passedAnalyzers)
                foreach (IStateLoader state in stateLoaders)
                {
                    analyzer.AggregateStateTo(aggregatedStates, state, aggregatedStates);
                }


            IEnumerable<IGroupingAnalyzer<IMetric>> groupingAnalyzers =
                passedAnalyzers.OfType<IGroupingAnalyzer<IMetric>>();

            IEnumerable<IAnalyzer<IMetric>> scanningAnalyzers = passedAnalyzers.Except(groupingAnalyzers);

            Dictionary<IAnalyzer<IMetric>, IMetric> nonGroupedResults = new Dictionary<IAnalyzer<IMetric>, IMetric>(
                scanningAnalyzers.SelectMany(analyzer =>
                {
                    IMetric metrics = analyzer.LoadStateAndComputeMetric(aggregatedStates);

                    if (saveStatesWith.HasValue)
                    {
                        analyzer.CopyStateTo(aggregatedStates, saveStatesWith.Value);
                    }

                    return new[] { new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer, metrics) };
                }));


            AnalyzerContext groupedResults;

            if (!groupingAnalyzers.Any())
            {
                groupedResults = AnalyzerContext.Empty();
            }
            else
            {
                groupedResults = groupingAnalyzers
                    .Select(analyzer => (IGroupingAnalyzer<IMetric>)analyzers)
                    .GroupBy(analyzer => analyzer.GroupingColumns().OrderBy(columnName => columnName))
                    .Select(analyzerForGrouping =>
                    {
                        FrequenciesAndNumRows state =
                            FindStateForParticularGrouping(analyzerForGrouping, aggregatedStates);
                        return RunAnalyzersForParticularGrouping(state, analyzerForGrouping, saveStatesWith);
                    }).Aggregate((x, y) => x + y);
            }

            AnalyzerContext results = preconditionFailures + new AnalyzerContext(nonGroupedResults) + groupedResults;
            SaveOrAppendResultsIfNecessary(results, metricsRepository, saveOrAppendResultsWithKey);

            return results;
        }

        private static FrequenciesAndNumRows FindStateForParticularGrouping(IEnumerable<IGroupingAnalyzer<IMetric>>
            analyzers, IStateLoader stateLoader)
        {
            /* One of the analyzers must have the state persisted */
            IEnumerable<Option<FrequenciesAndNumRows>> states = analyzers.Select(analyzer =>
                stateLoader.Load<FrequenciesAndNumRows>(
                    (Analyzer<FrequenciesAndNumRows, IMetric>)analyzer));


            return states.FirstOrDefault().Value;
        }


        private static void SaveOrAppendResultsIfNecessary(
            AnalyzerContext resultingAnalyzerContext,
            Option<IMetricsRepository> metricsRepository,
            Option<ResultKey> saveOrAppendResultsWithKey) =>
            metricsRepository.OnSuccess(repository =>
            {
                saveOrAppendResultsWithKey.OnSuccess(key =>
                {
                    AnalyzerContext valueToSave = repository.LoadByKey(key).GetOrElse(AnalyzerContext.Empty());

                    Dictionary<string, IAnalyzer<IMetric>> dictEquality =
                        valueToSave.MetricMap.ToDictionary(pair => pair.Key.ToString(), pair => pair.Key);

                    resultingAnalyzerContext.MetricMap.ToList().ForEach(keyValuePair =>
                    {
                        if (dictEquality.ContainsKey(keyValuePair.Key.ToString()) &&
                            valueToSave.MetricMap.ContainsKey(dictEquality[keyValuePair.Key.ToString()]))
                        {
                            valueToSave.MetricMap[dictEquality[keyValuePair.Key.ToString()]] = keyValuePair.Value;
                        }
                        else
                        {
                            valueToSave.MetricMap.Add(keyValuePair.Key, keyValuePair.Value);
                        }
                    });

                    repository.Save(saveOrAppendResultsWithKey.Value, valueToSave);
                });
            });


        private static void SaveJsonOutputsToFilesystemIfNecessary(
            AnalysisRunnerFileOutputOptions fileOutputOptions,
            AnalyzerContext analyzerContext)
        {
            //TODO implement this part
        }

        private static Option<Exception>
            FindFirstFailing(StructType schema, IEnumerable<Action<StructType>> conditions) =>
            conditions
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
                .FirstOrDefault(exception => exception.HasValue);

        private static AnalyzerContext ComputePreconditionFailureMetrics(
            IEnumerable<IAnalyzer<IMetric>> failedAnalyzers, StructType schema)
        {
            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> failures = failedAnalyzers.Select(analyzer =>
            {
                Exception firstException = FindFirstFailing(schema, analyzer.Preconditions()).Value;
                return new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer,
                    analyzer.ToFailureMetric(firstException));
            });

            return new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>(failures));
        }

        private static Option<string> GetFilterCondition(IAnalyzer<IMetric> analyzer)
        {
            if (analyzer is IFilterableAnalyzer fa)
            {
                return fa.FilterCondition();
            }

            return Option<string>.None;
        }


        private static AnalyzerContext RunScanningAnalyzers(DataFrame dataFrame,
            IEnumerable<IAnalyzer<IMetric>> analyzers,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateTo
        )
        {
            IEnumerable<IScanSharableAnalyzer<IState, IMetric>> sharable =
                analyzers.OfType<IScanSharableAnalyzer<IState, IMetric>>();
            IEnumerable<IAnalyzer<IMetric>> others = analyzers.Except(sharable);

            AnalyzerContext sharedResults;
            if (sharable.Any())
            {
                IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> metricsByAnalyzer;

                try
                {
                    IEnumerable<Column> aggregations = sharable
                        .SelectMany(analyzer => analyzer.AggregationFunctions());

                    int i = 0;


                    List<int> offsets = sharable.Select(analyzer =>
                    {
                        i += analyzer.AggregationFunctions().Count();
                        return i;
                    }).ToList();

                    offsets.Insert(0, 0);

                    Row results = dataFrame.Agg(aggregations.FirstOrDefault(), aggregations.Skip(1).ToArray()).Collect()
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

                Dictionary<IAnalyzer<IMetric>, IMetric> metricsByAnalyzerDict =
                    new Dictionary<IAnalyzer<IMetric>, IMetric>(metricsByAnalyzer);
                sharedResults = new AnalyzerContext(metricsByAnalyzerDict);
            }
            else
            {
                sharedResults = AnalyzerContext.Empty();
            }

            Dictionary<IAnalyzer<IMetric>, IMetric> otherMetrics = new Dictionary<IAnalyzer<IMetric>, IMetric>(
                others.Select(analyzer =>
                    new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer,
                        analyzer.Calculate(dataFrame, aggregateWith, saveStateTo))));

            return sharedResults + new AnalyzerContext(otherMetrics);
        }


        private static (long, AnalyzerContext) RunGroupingAnalyzers(
            DataFrame dataFrame,
            IEnumerable<string> groupingColumns,
            Option<string> filterConditions,
            IEnumerable<IGroupingAnalyzer<IMetric>> analyzers,
            Option<IStateLoader> aggregateWith,
            Option<IStatePersister> saveStateTo,
            StorageLevel storageLevelOfGroupedDataForMultiplePasses,
            Option<double> numRowsOfData
        )
        {
            FrequenciesAndNumRows frequenciesAndNumRows =
                FrequencyBasedAnalyzer.ComputeFrequencies(dataFrame, groupingColumns, filterConditions);

            Analyzer<FrequenciesAndNumRows, IMetric> sampleAnalyzer =
                analyzers.First() as Analyzer<FrequenciesAndNumRows, IMetric>;

            Option<FrequenciesAndNumRows> previousFrequenciesAndNumRows = aggregateWith
                .Select(stateLoader => stateLoader.Load<FrequenciesAndNumRows>(sampleAnalyzer))
                .GetOrElse(Option<FrequenciesAndNumRows>.None);

            if (previousFrequenciesAndNumRows.HasValue)
            {
                frequenciesAndNumRows =
                    (FrequenciesAndNumRows)frequenciesAndNumRows.Sum(previousFrequenciesAndNumRows.Value);
            }


            AnalyzerContext results = RunAnalyzersForParticularGrouping(frequenciesAndNumRows, analyzers, saveStateTo);

            return (frequenciesAndNumRows.NumRows, results);
        }

        private static AnalyzerContext RunAnalyzersForParticularGrouping(
            FrequenciesAndNumRows frequenciesAndNumRows,
            IEnumerable<IGroupingAnalyzer<IMetric>> analyzers,
            Option<IStatePersister> saveStatesTo
        )
        {
            long numRows = frequenciesAndNumRows.NumRows;

            IEnumerable<ScanShareableFrequencyBasedAnalyzer> shareable = analyzers
                .OfType<ScanShareableFrequencyBasedAnalyzer>();

            IEnumerable<IGroupingAnalyzer<IMetric>> others = analyzers.Except(shareable);

            if (!others.Any())
            {
                frequenciesAndNumRows.Frequencies.Persist(); // TODO: storageLevelOfGroupedDataForMultiplePasses
            }

            IEnumerable<ScanShareableFrequencyBasedAnalyzer> sharableAnalyzers = shareable;


            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> metricsByAnalyzer;

            if (!sharableAnalyzers.Any())
            {
                metricsByAnalyzer = new List<KeyValuePair<IAnalyzer<IMetric>, IMetric>>();
            }
            else
            {
                try
                {
                    IEnumerable<Column> aggregations = sharableAnalyzers
                        .SelectMany(analyzer => analyzer.AggregationFunctions(numRows));

                    int i = 0;
                    List<int> offsets = sharableAnalyzers.Select(analyzer =>
                    {
                        i += analyzer.AggregationFunctions(numRows).Count();
                        return i;
                    }).ToList();

                    offsets.Insert(0, 0);

                    Row results = frequenciesAndNumRows.Frequencies
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
                    metricsByAnalyzer = sharableAnalyzers.Select(analyzer =>
                        new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer, analyzer.ToFailureMetric(e)));
                }
            }

            IEnumerable<KeyValuePair<IAnalyzer<IMetric>, IMetric>> otherMetrics;

            try
            {
                otherMetrics = others
                    .Select(analyzer => (FrequencyBasedAnalyzer)analyzer)
                    .Select(analyzer => new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer,
                        analyzer.ComputeMetricFrom(new Option<FrequenciesAndNumRows>(frequenciesAndNumRows))));
            }
            catch (Exception e)
            {
                otherMetrics = others.Select(analyzer =>
                    new KeyValuePair<IAnalyzer<IMetric>, IMetric>(analyzer, analyzer.ToFailureMetric(e)));
            }

            saveStatesTo.Select(statePersister =>
                statePersister.Persist(new Option<IAnalyzer<IMetric>>(analyzers.First()), frequenciesAndNumRows));
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
        public ResultKey(long dataSetDate, Dictionary<string, string> tags)
        {
            DataSetDate = dataSetDate;
            Tags = tags;
        }

        public ResultKey()
        {
        }

        public long DataSetDate { get; set; }
        public Dictionary<string, string> Tags { get; set; }
    }


    //TODO: Missing API from dotnet/spark, see: https://github.com/dotnet/spark/issues/617
    public enum StorageLevel
    {
        MEMORY_AND_DISK = 0
    }
}
