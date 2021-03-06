using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Repository;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using StorageLevel = deequ.Analyzers.Runners.StorageLevel;

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class SimpleResultSerdeTest
    {
        public SimpleResultSerdeTest(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;


        [Fact]
        public void serialize_and_deserialize_success_metric_results_with_tags()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);

            Analysis analysis = new Analysis()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] { "item" }, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att1"))
                .AddAnalyzer(Initializers.Uniqueness("att1", Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] { "att1" }, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att2"))
                .AddAnalyzer(Initializers.Uniqueness(new[] { "att2" }))
                .AddAnalyzer(Initializers.MutualInformation("att1", "att2"))
                .AddAnalyzer(Initializers.MinLength("att1"))
                .AddAnalyzer(Initializers.MaxLength("att1"));

            AnalyzerContext analysisContext = analysis.Run(df, Option<IStateLoader>.None,
                Option<IStatePersister>.None);

            long dateTime = DateTime.UtcNow.Ticks;

            string successMetricsResultJson =
                new AnalysisResult(
                    new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "EU" } }),
                    analysisContext
                ).GetSuccessMetricsAsJson(Enumerable.Empty<IAnalyzer<IMetric>>(),
                    Enumerable.Empty<string>());

            SimpleMetricOutput[] result =
                JsonSerializer.Deserialize<SimpleMetricOutput[]>(successMetricsResultJson,
                    SerdeExt.GetDefaultOptions());
            result.ShouldNotBeNull();

            foreach (SimpleMetricOutput metric in result)
            {
                metric.Entity.ShouldNotBeNullOrEmpty();
                metric.Instance.ShouldNotBeNullOrEmpty();
                metric.Name.ShouldNotBeNullOrEmpty();
                metric.Value.ShouldNotBe(0);
            }
        }
    }
}
