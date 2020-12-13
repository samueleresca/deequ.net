using System;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static deequ.Analyzers.Initializers;


namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class UniquenessAnalyzer
    {
        public UniquenessAnalyzer(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void compute_correct_metrics_complete()
        {
            DataFrame complete = FixtureSupport.GetDFFull(_session);

            DoubleMetric attr1 = Uniqueness(new[] { "att1" }).Calculate(complete);
            DoubleMetric attr2 = Uniqueness(new[] { "att2" }).Calculate(complete);

            DoubleMetric expected1 = DoubleMetric.Create(MetricEntity.Column, "Uniqueness", "att1", 0.25);
            DoubleMetric expected2 = DoubleMetric.Create(MetricEntity.Column, "Uniqueness", "att2", 0.25);

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
        public void compute_correct_metrics_missing()
        {
            DataFrame missing = FixtureSupport.GetDFMissing(_session);

            DoubleMetric attr1 = Uniqueness(new[] { "att1" }).Calculate(missing);
            DoubleMetric attr2 = Uniqueness(new[] { "att2" }).Calculate(missing);

            DoubleMetric expected1 = DoubleMetric.Create(MetricEntity.Column, "Uniqueness", "att1", 0.0);
            DoubleMetric expected2 = DoubleMetric.Create(MetricEntity.Column, "Uniqueness", "att2", 0.0);

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
        public void compute_correct_metrics_on_multiple_columns()
        {
            DataFrame complete = FixtureSupport.GetDFWithUniqueColumns(_session);

            DoubleMetric attr1 = Uniqueness(new[] { "unique" }).Calculate(complete);
            DoubleMetric attr2 = Uniqueness(new[] { "uniqueWithNulls" }).Calculate(complete);

            DoubleMetric expected1 = DoubleMetric.Create(MetricEntity.Column, "Uniqueness", "unique", 1.0);
            DoubleMetric expected2 = DoubleMetric.Create(MetricEntity.Column, "Uniqueness", "uniqueWithNulls", 1.0);

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
        public void fail_on_wrong_column_input()
        {
            DataFrame complete = FixtureSupport.GetDFMissing(_session);

            DoubleMetric attr1 = Uniqueness(new[] { "nonExistingColumn" }).Calculate(complete);
            DoubleMetric attr2 = Uniqueness(new[] { "nonExistingColumn", "unique" }).Calculate(complete);

            DoubleMetric expected1 = DoubleMetric.Create(MetricEntity.Column, "Uniqueness", "nonExistingColumn",
                new Try<double>(new Exception()));
            DoubleMetric expected2 = DoubleMetric.Create(MetricEntity.Multicolumn, "Uniqueness", "nonExistingColumn,unique",
                new Try<double>(new Exception()));

            attr1.MetricEntity.ShouldBe(expected1.MetricEntity);
            attr1.Instance.ShouldBe(expected1.Instance);
            attr1.Name.ShouldBe(expected1.Name);
            attr1.Value.IsSuccess.ShouldBeFalse();

            attr2.MetricEntity.ShouldBe(expected2.MetricEntity);
            attr2.Instance.ShouldBe(expected2.Instance);
            attr2.Name.ShouldBe(expected2.Name);
            attr2.Value.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void uniqueness_correctly_tostring_instances()
        {
            Uniqueness(new[] { "att1", "att2" }).ToString().ShouldBe("Uniqueness(List(att1,att2),None)");
        }
    }
}
