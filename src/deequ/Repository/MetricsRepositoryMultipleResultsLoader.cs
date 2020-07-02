using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Extensions;
using xdeequ.Metrics;
using static Microsoft.Spark.Sql.Functions;

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
                .Aggregate((x, y) => { return DataFrameUnion(x, y); });
        }

        public string GetSuccessMetricsAsJson(IEnumerable<string> withTags)
        {
            IEnumerable<AnalysisResult> analysisResults = Get();

            if (!analysisResults.Any())
            {
                return new AnalysisResult(new ResultKey(0, new Dictionary<string, string>()),
                        new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>()))
                    .GetSuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>(), withTags);
            }

            return analysisResults
                .Select(x => x.GetSuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>(), withTags))
                .Aggregate((x, y) => JsonUnion(x, y));
        }

        private DataFrame DataFrameUnion(DataFrame dataFrameOne, DataFrame dataFrameTwo)
        {
            string[] columnsOne = dataFrameOne.Columns().ToArray();
            string[] columnsTwo = dataFrameTwo.Columns().ToArray();

            IEnumerable<string> columnTotal = columnsOne.Concat(columnsTwo).Distinct();

            return dataFrameOne
                .Select(WithAllColumns(columnsOne, columnTotal.ToArray()).ToArray())
                .Union(dataFrameTwo.Select(WithAllColumns(columnsTwo, columnTotal)));
        }

        private string JsonUnion(string jsonOne, string jsonTwo)
        {
            Dictionary<string, object>[] objectOne = JsonSerializer.Deserialize<Dictionary<string, object>[]>(jsonOne);
            Dictionary<string, object>[] objectTwo = JsonSerializer.Deserialize<Dictionary<string, object>[]>(jsonTwo);

            IEnumerable<string> columnsTotal = objectOne.FirstOrDefault()?.Keys
                .Concat(objectTwo.FirstOrDefault()?.Keys ?? Enumerable.Empty<string>());
            IEnumerable<Dictionary<string, object>> unioned = objectOne.Concat(objectTwo).Select(x =>
            {
                Dictionary<string, object> columnsToAdd = new Dictionary<string, object>();

                foreach (string column in columnsTotal.Distinct())
                {
                    columnsToAdd.Add(column, null);
                }

                x.Merge(columnsToAdd);
                return x;
            });

            return JsonSerializer.Serialize(unioned);
        }

        private static Column[] WithAllColumns(IEnumerable<string> myCols, IEnumerable<string> allCols) =>
            allCols.Select(x =>
            {
                if (myCols.Contains(x))
                {
                    return Column(x);
                }

                return Lit(null).As(x);
            }).ToArray();
    }
}
