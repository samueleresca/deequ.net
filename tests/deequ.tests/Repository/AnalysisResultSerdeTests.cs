using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Repository;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Repository
{
    public class AnalysisResultSerdeTests
    {
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
        public void analysis_results_serialization_with_mixed_Values_should_fail()
        {
            ArgumentException sampleException = new ArgumentException("Some");
            AnalyzerContext analyzerContextWithMixedValues = new AnalyzerContext(
                new Dictionary<IAnalyzer<IMetric>, IMetric>
                {
                    {
                        Initializers.Size(Option<string>.None),
                        new DoubleMetric(Entity.Column, "Size", "*", Try<double>.From(() => 5.0))
                    },
                    {
                        Initializers.Completeness("ColumnA"), new DoubleMetric(Entity.Column, "Completeness", "ColumnA",
                            Try<double>.From(() => throw sampleException))
                    }
                });

            long dateTime = DateTime.UtcNow.ToBinary();
            ResultKey resultKeyOne = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "EU" } });
            ResultKey resultKeyTwo = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "NA" } });

            AnalysisResult analysisResultOne = new AnalysisResult(resultKeyOne, analyzerContextWithMixedValues);
            AnalysisResult analysisResultTwo = new AnalysisResult(resultKeyTwo, analyzerContextWithMixedValues);

            AssertCorrectlyConvertsAnalysisResults(new[] { analysisResultOne, analysisResultTwo }, true);
        }

        [Fact]
        public void analysis_results_serialization_with_successful_values_should_work()
        {
            AnalyzerContext analyzerContextWithAllSuccValues = new AnalyzerContext(
                new Dictionary<IAnalyzer<IMetric>, IMetric>
                {
                    {
                        Initializers.Size(Option<string>.None),
                        new DoubleMetric(Entity.Column, "Size", "*", new Try<double>(5.0))
                    },
                    {
                        Initializers.Completeness("ColumnA"),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    //TODO: ApproxCountDistinct
                    //TODO: CountDistinct
                    {
                        Initializers.Distinctness(new[] {"columnA", "columnB"}, Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    //TODO: Correlation
                    {
                        Initializers.UniqueValueRatio(new[] {"columnA", "columnB"}, Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    //TODO: Correlation
                    {
                        Initializers.Uniqueness(new[] {"ColumnA"}, Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.Uniqueness(new[] {"ColumnA", "ColumnB"}, Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.Histogram("ColumnA"), new HistogramMetric("ColumnA", new Try<Distribution>(
                            new Distribution(
                                new Dictionary<string, DistributionValue> {{"some", new DistributionValue(10, .5)}},
                                10)))
                    },
                    {
                        Initializers.Histogram("ColumnA", Option<Func<Column, Column>>.None), new HistogramMetric(
                            "ColumnA", new Try<Distribution>(
                                new Distribution(
                                    new Dictionary<string, DistributionValue>
                                    {
                                        {"some", new DistributionValue(10, .5)},
                                        {"other", new DistributionValue(0, 0)}
                                    }, 10)))
                    },
                    {
                        Initializers.Histogram("ColumnA", Option<string>.None, 5), new HistogramMetric("ColumnA",
                            new Try<Distribution>(
                                new Distribution(
                                    new Dictionary<string, DistributionValue> {{"some", new DistributionValue(10, .5)}},
                                    10)))
                    },
                    {
                        Initializers.Entropy("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.MutualInformation(new[] {"ColumnA", "ColumnB"}, Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.Minimum("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.Maximum("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.Mean("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.Sum("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.StandardDeviation("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.DataType("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "Completeness", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.MinLength("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "MinLength", "ColumnA", new Try<double>(5.0))
                    },
                    {
                        Initializers.MaxLength("ColumnA", Option<string>.None),
                        new DoubleMetric(Entity.Column, "MaxLength", "ColumnA", new Try<double>(5.0))
                    }
                });

            long dateTime = DateTime.UtcNow.Ticks;

            ResultKey resultKeyOne = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "EU" } });
            ResultKey resultKeyTwo = new ResultKey(dateTime, new Dictionary<string, string> { { "Region", "NA" } });

            AnalysisResult analysisResultOne = new AnalysisResult(resultKeyOne, analyzerContextWithAllSuccValues);
            AnalysisResult analysisResultTwo = new AnalysisResult(resultKeyTwo, analyzerContextWithAllSuccValues);

            AssertCorrectlyConvertsAnalysisResults(new[] { analysisResultOne, analysisResultTwo });
        }


        [Fact(Skip = "ApproxQuantile not implemented")]
        public void serialization_of_ApproxQuantile_should_correctly_restore_id()
        {
        }
    }
}
