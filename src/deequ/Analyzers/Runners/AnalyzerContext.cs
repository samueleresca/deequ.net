using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers.Runners
{
    public class AnalyzerContext : IEquatable<AnalyzerContext>
    {
        public Dictionary<IAnalyzer<IMetric>, IMetric> MetricMap;

        public AnalyzerContext(Dictionary<IAnalyzer<IMetric>, IMetric> metricMap) => MetricMap = metricMap;

        public bool Equals(AnalyzerContext other) =>
            MetricMap.OrderBy(keyValuePair => keyValuePair.Key.ToString())
                .SequenceEqual(other.MetricMap.OrderBy(keyValuePair => keyValuePair.Key.ToString()));

        public static AnalyzerContext Empty() => new AnalyzerContext(new Dictionary<IAnalyzer<IMetric>, IMetric>());

        public static AnalyzerContext operator +(AnalyzerContext current, AnalyzerContext other)
        {
            current.MetricMap.Merge(other.MetricMap);
            return new AnalyzerContext(current.MetricMap);
        }

        public Option<IMetric> Metric(IAnalyzer<IMetric> analyzer)
        {
            try
            {
                return new Option<IMetric>(MetricMap[analyzer]);
            }
            catch (KeyNotFoundException e)
            {
                return Option<IMetric>.None;
            }
        }

        public DataFrame SuccessMetricsAsDataFrame(
            SparkSession sparkSession,
            IEnumerable<IAnalyzer<IMetric>> forAnalyzers)
        {
            IEnumerable<GenericRow> metricList =
                GetSimplifiedMetricOutputForSelectedAnalyzers(forAnalyzers)
                    .Select(simpleMetric => new GenericRow(new object[] { simpleMetric.Entity.ToString(), simpleMetric.Instance, simpleMetric.Name, simpleMetric.Value }));

            DataFrame df = sparkSession.CreateDataFrame(metricList,
                new StructType(new[]
                {
                    new StructField("entity", new StringType()), new StructField("instance", new StringType()),
                    new StructField("name", new StringType()), new StructField("value", new DoubleType())
                }));
            return df;
        }

        public string SuccessMetricsAsJson(IEnumerable<IAnalyzer<IMetric>> forAnalyzers)
        {
            SimpleMetricOutput[] metricsList = GetSimplifiedMetricOutputForSelectedAnalyzers(forAnalyzers).ToArray();
            return JsonSerializer.Serialize(metricsList, SerdeExt.GetDefaultOptions());
        }

        private IEnumerable<SimpleMetricOutput> GetSimplifiedMetricOutputForSelectedAnalyzers(
            IEnumerable<IAnalyzer<IMetric>> forAnalyzers) =>
            MetricMap
                .Where((pair, i) => !forAnalyzers.Any() || forAnalyzers.Contains(pair.Key))
                .Where((pair, i) =>
                {
                    DoubleMetric dm = pair.Value as DoubleMetric;
                    return dm.Value.IsSuccess;
                })
                .SelectMany(pair =>
                {
                    DoubleMetric dm = pair.Value as DoubleMetric;
                    return dm.Flatten().Select(doubleMetric => RenameMetric(doubleMetric, DescribeAnalyzer(pair.Key)));
                })
                .Select(doubleMetric => new SimpleMetricOutput(doubleMetric));

        private static string DescribeAnalyzer(IAnalyzer<IMetric> analyzer)
        {
            string name = analyzer.GetType().Name;

            Option<string> filter = Option<string>.None;

            if (analyzer is IFilterableAnalyzer filterable)
            {
                filter = filterable.FilterCondition();
            }

            return filter.Select(filter => $"{name} (where: {filter} )").GetOrElse(name);
        }

        private static DoubleMetric RenameMetric(DoubleMetric doubleMetric, string newName) =>
            new DoubleMetric(doubleMetric.MetricEntity, newName, doubleMetric.Instance, doubleMetric.Value);

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != GetType())
            {
                return false;
            }

            return Equals((AnalyzerContext)obj);
        }

        public override int GetHashCode() => MetricMap != null ? MetricMap.GetHashCode() : 0;
    }
}
