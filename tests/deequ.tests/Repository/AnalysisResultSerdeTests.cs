using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using deequ;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Extensions;
using deequ.Interop;
using deequ.Metrics;
using deequ.Repository;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using Shouldly;
using Xunit;

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class AnalysisResultSerdeTests
    {
        public AnalysisResultSerdeTests(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;
        private void AssertCorrectlyConvertsAnalysisResults(IEnumerable<AnalysisResult> analysisResults,
            bool shouldFail = false)
        {
            if (shouldFail)
            {
                Assert.Throws<ArgumentException>(() =>
                    JsonSerializer.Serialize(analysisResults, SerdeExt.GetDefaultOptions()));
                return;
            }

            string serialized = JsonSerializer.Serialize(analysisResults, SerdeExt.GetDefaultOptions());
            AnalysisResult[] deserialized =
                JsonSerializer.Deserialize<AnalysisResult[]>(serialized, SerdeExt.GetDefaultOptions());

            analysisResults.Count().ShouldBe(deserialized.Count());
        }

        [Fact]
        public void analysis_results_serialization_with_mixed_Values()
        {

            DataFrame df = FixtureSupport.GetDFFull(_session);
            ArgumentException sampleException = new ArgumentException("Some");

            AnalysisJvm analysis = new AnalysisJvm()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("ColumnA"));

            AnalyzerContext context =  analysis.Run(df);

            long dateTime = DateTime.UtcNow.ToBinary();
            ResultKey resultKeyOne = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "EU" } });
            ResultKey resultKeyTwo = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "NA" } });

            AnalysisResult analysisResultOne = new AnalysisResult(resultKeyOne, context);
            AnalysisResult analysisResultTwo = new AnalysisResult(resultKeyTwo, context);

            AssertCorrectlyConvertsAnalysisResults(new[] { analysisResultOne, analysisResultTwo }, false);
        }

        [Fact]
        public void analysis_results_serialization_with_successful_values_should_work()
        {

            DataFrame df = FixtureSupport.GetDFFull(_session);
            AnalysisJvm analysisJvm =  new AnalysisJvm()
                .AddAnalyzers(
                new List<IAnalyzer<IMetric>>
                {
                        Initializers.Size(Option<string>.None),
                        Initializers.Completeness("ColumnA"),
                        Initializers.Distinctness(new[] {"columnA", "columnB"}, Option<string>.None),
                        Initializers.UniqueValueRatio(new[] {"columnA", "columnB"}, Option<string>.None),
                        Initializers.Correlation("firstColumn", "secondColumn", "test"),
                        Initializers.Uniqueness(new[] {"ColumnA"}, Option<string>.None),
                        Initializers.Uniqueness(new[] {"ColumnA", "ColumnB"}, Option<string>.None),
                        Initializers.Histogram("ColumnA"),
                        Initializers.Entropy("ColumnA", Option<string>.None),
                        Initializers.MutualInformation(new[] {"ColumnA", "ColumnB"}, Option<string>.None),
                        Initializers.Minimum("ColumnA", Option<string>.None),
                        Initializers.Maximum("ColumnA", Option<string>.None),
                        Initializers.Mean("ColumnA", Option<string>.None),
                        Initializers.Sum("ColumnA", Option<string>.None),
                        Initializers.StandardDeviation("ColumnA", Option<string>.None),
                        Initializers.DataType("ColumnA", Option<string>.None),
                        Initializers.MinLength("ColumnA", Option<string>.None),
                        Initializers.MaxLength("ColumnA", Option<string>.None)
                    });


            AnalyzerContext  context = analysisJvm.Run(df);
            long dateTime = DateTime.UtcNow.Ticks;

            ResultKey resultKeyOne = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "EU" } });
            ResultKey resultKeyTwo = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "NA" } });

            AnalysisResult analysisResultOne = new AnalysisResult(resultKeyOne, context);
            AnalysisResult analysisResultTwo = new AnalysisResult(resultKeyTwo, context);

            AssertCorrectlyConvertsAnalysisResults(new[] { analysisResultOne, analysisResultTwo });
        }


        [Fact(Skip = "ApproxQuantile not implemented")]
        public void serialization_of_ApproxQuantile_should_correctly_restore_id()
        {
        }
    }
}
