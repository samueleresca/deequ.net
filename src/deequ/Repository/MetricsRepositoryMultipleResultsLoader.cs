using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Spark.Sql;
using static Microsoft.Spark.Sql.Functions;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Extensions;
using xdeequ.Metrics;

namespace xdeequ.Repository
{

    public abstract class MetricsRepositoryMultipleResultsLoader : IMetricRepositoryMultipleResultsLoader
    {
        public abstract IMetricRepositoryMultipleResultsLoader WithTagValues(Dictionary<string, string> tagValues);

        public abstract IMetricRepositoryMultipleResultsLoader ForAnalyzers(IEnumerable<IAnalyzer<IMetric>> analyzers);

        public abstract IMetricRepositoryMultipleResultsLoader After(long dateTime);

        public abstract IMetricRepositoryMultipleResultsLoader Before(long dateTime);

        public abstract IEnumerable<AnalysisResult> Get();


        public DataFrame GetSuccessMetricsAsDataFrame(SparkSession session, IEnumerable<string> withTags)
        {
            IEnumerable<AnalysisResult> analysisResults = Get();

            if (!analysisResults.Any())
            {
                return new AnalysisResult(new ResultKey(0, new Dictionary<string, string>()),
                          new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>()))
                      .GetSuccessMetricsAsDataFrame(session, Enumerable.Empty<IAnalyzer<IMetric>>(), withTags);
            }

            return analysisResults
                .Select(x => x.GetSuccessMetricsAsDataFrame(session,
                    Enumerable.Empty<IAnalyzer<IMetric>>(), withTags))
                .Aggregate((x, y) => DataFrameUnion(x, y));

        }

        public string GetSuccessMetricsAsJson(SparkSession session, IEnumerable<string> withTags)
        {
            var analysisResults = Get();

            if (!analysisResults.Any())
            {
                return new AnalysisResult(new ResultKey(0, new Dictionary<string, string>()),
                        new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>()))
                    .GetSuccessMetricsAsJson(session, Enumerable.Empty<IAnalyzer<IMetric>>(), withTags);
            }

            return analysisResults
                .Select(x => x.GetSuccessMetricsAsJson(session,
                    Enumerable.Empty<IAnalyzer<IMetric>>(), withTags))
                .Aggregate((x, y) => JsonUnion(x, y));

        }

        private DataFrame DataFrameUnion(DataFrame dataFrameOne, DataFrame dataFrameTwo)
        {
            var columnsOne = dataFrameOne.Columns().ToArray();
            var columnsTwo = dataFrameTwo.Columns().ToArray();

            var columnTotal = columnsOne.Concat(columnsTwo);

            return dataFrameOne
                .Select(WithAllColumns(columnsOne, columnTotal).ToArray())
                .Union(dataFrameTwo.Select((Column[])WithAllColumns(columnsTwo, columnTotal)));
        }

        private string JsonUnion(string jsonOne, string jsonTwo)
        {

            var objectOne = JsonSerializer.Deserialize<Dictionary<string, object>[]>(jsonOne);
            var objectTwo = JsonSerializer.Deserialize<Dictionary<string, object>[]>(jsonTwo);

            var columnsTotal = objectOne.FirstOrDefault()?.Keys.Concat(objectTwo.FirstOrDefault()?.Keys ?? Enumerable.Empty<string>());
            IEnumerable<Dictionary<string, object>> unioned = objectOne.Concat(objectTwo).Select(x =>
            {
                var columnsToAdd = new Dictionary<string, object>();

                foreach (var column in columnsTotal.Distinct())
                {
                    columnsToAdd.Add(column, null);
                }

                x.Merge(columnsToAdd);
                return x;
            });

            return JsonSerializer.Serialize(unioned);
        }
        private static IEnumerable<Column> WithAllColumns(IEnumerable<string> myCols, IEnumerable<string> allCols)
        {
            return allCols.Select(x =>
            {
                if (myCols.Contains(x))
                {
                    return Column(x);
                }

                return Lit(null).As(x);

            });
        }
    }
}
