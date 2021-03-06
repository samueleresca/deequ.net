using System.Linq;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static deequ.Analyzers.Initializers;


namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class CompletenessAnalyzer
    {
        public CompletenessAnalyzer(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics_missing()
        {
            DataFrame missing = FixtureSupport.GetDFMissing(_session);

            Completeness("someMissingColumn").Preconditions().Count().ShouldBe(2);

            DoubleMetric attr1 = Completeness("att1").Calculate(missing);
            DoubleMetric attr2 = Completeness("att2").Calculate(missing);

            DoubleMetric expected1 = DoubleMetric.Create(MetricEntity.Column, "Completeness", "att1", 0.5);
            DoubleMetric expected2 = DoubleMetric.Create(MetricEntity.Column, "Completeness", "att2", 0.75);

            attr1.MetricEntity.ShouldBe(expected1.MetricEntity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.Get().ShouldBe(expected1.Value.Get());

            attr2.MetricEntity.ShouldBe(expected2.MetricEntity);
            attr2.Instance.ShouldBe(expected2.Instance);
            attr2.Name.ShouldBe(expected2.Name);
            attr2.Value.Get().ShouldBe(expected2.Value.Get());
        }

        [Fact]
        public void compute_correct_metrics_missing_with_filtering()
        {
            DataFrame missing = FixtureSupport.GetDFMissing(_session);

            DoubleMetric attr1 = Completeness("att1", new Option<string>("item in ('1', '2')"))
                .Calculate(missing);

            DoubleMetric expected1 = DoubleMetric.Create(MetricEntity.Column, "Completeness", "att1", 1.0);

            attr1.MetricEntity.ShouldBe(expected1.MetricEntity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.Get().ShouldBe(expected1.Value.Get());
        }

        [Fact]
        public void fail_on_nested_column_input()
        {
            DataFrame missing = FixtureSupport.GetDFMissing(_session);

            DoubleMetric attr1 = Completeness("source").Calculate(missing);
            attr1.Value.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void completeness_correctly_tostring_instances()
        {
            Completeness("source").ToString().ShouldBe("Completeness(source,None)");
        }

        [Fact]
        public void fail_on_wrong_column_input()
        {
            DataFrame missing = FixtureSupport.GetDFMissing(_session);

            DoubleMetric attr1 = Completeness("someMissingColumn").Calculate(missing);

            attr1.MetricEntity.ShouldBe(MetricEntity.Column);
            attr1.Instance.ShouldBe("someMissingColumn");
            attr1.Name.ShouldBe("Completeness");
            attr1.Value.IsSuccess.ShouldBeFalse();
        }
    }
}
