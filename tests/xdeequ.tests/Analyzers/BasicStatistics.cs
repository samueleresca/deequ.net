using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using static xdeequ.Analyzers.Initializers;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class BasicStatistics
    {
        public BasicStatistics(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void compute_maximum_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Try<double> result = Maximum("att1").Calculate(df).Value;
            result.Get().ShouldBe(6.0);
        }

        [Fact]
        public void compute_maximum_correctly_for_numeric_data_with_filtering()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Try<double> result = Maximum("att1", "item != '6'").Calculate(df).Value;
            result.Get().ShouldBe(5.0);
        }

        [Fact]
        public void compute_maxlength_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            Try<double> result = MaxLength("att1").Calculate(df).Value;
            result.Get().ShouldBe(4.0);
        }

        [Fact]
        public void compute_maxlength_correctly_for_numeric_data_with_filtering()
        {
            DataFrame df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            Try<double> result = MaxLength("att1", "att1 != 'dddd'").Calculate(df).Value;
            result.Get().ShouldBe(3.0);
        }

        [Fact]
        public void compute_mean_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            DoubleMetric result = Mean("att1").Calculate(df);
            result.Value.Get().ShouldBe(3.5);
        }

        [Fact]
        public void compute_mean_correctly_for_numeric_data_with_where_predicate()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            DoubleMetric result = Mean("att1", "item != '6'").Calculate(df);
            result.Value.Get().ShouldBe(3.0);
        }

        [Fact]
        public void compute_minimum_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Try<double> result = Minimum("att1").Calculate(df).Value;
            result.Get().ShouldBe(1.0);
        }

        [Fact]
        public void compute_minimum_correctly_for_numeric_data_with_filtering()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Try<double> result = Minimum("att1", "item != '1'").Calculate(df).Value;
            result.Get().ShouldBe(2.0);
        }

        [Fact]
        public void compute_minlength_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            Try<double> result = MinLength("att1").Calculate(df).Value;
            result.Get().ShouldBe(0.0);
        }

        [Fact]
        public void compute_minlength_correctly_for_numeric_data_with_filtering()
        {
            DataFrame df = FixtureSupport.GetDfWithVariableStringLengthValues(_session);
            Try<double> result = MinLength("att1", "att1 != ''").Calculate(df).Value;
            result.Get().ShouldBe(1.0);
        }

        [Fact]
        public void compute_standard_deviation_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Try<double> result = StandardDeviation("att1").Calculate(df).Value;

            result.Get().ShouldBe(1.707825127659933);
        }

        [Fact]
        public void compute_sum_correctly_for_numeric_data()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Sum("att1").Calculate(df).Value.Get().ShouldBe(21);
        }

        [Fact]
        public void fail_to_compute_maximum_for_non_numeric_type()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);
            Try<double> result = Maximum("att1").Calculate(df).Value;
            result.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void fail_to_compute_maxlength_for_non_numeric_type()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Try<double> result = MinLength("att1").Calculate(df).Value;
            result.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void fail_to_compute_mean_for_non_numeric_type()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);
            DoubleMetric result = Mean("att1").Calculate(df);
            result.Value.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void fail_to_compute_minimum_for_non_numeric_type()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);
            Try<double> result = Minimum("att1").Calculate(df).Value;
            result.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void fail_to_compute_minlength_for_non_numeric_type()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);
            Try<double> result = MinLength("att1").Calculate(df).Value;
            result.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void fail_to_compute_standard_deviation_for_non_numeric_type()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);
            Try<double> result = StandardDeviation("att1").Calculate(df).Value;
            result.IsSuccess.ShouldBeFalse();
        }

        [Fact]
        public void fail_to_compute_sum_for_non_numeric_type()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);
            Sum("att1").Calculate(df).Value.IsSuccess.ShouldBeFalse();
        }

        [Fact(Skip = "Support decimal columns")]
        public void should_work_correctly_on_decimal_columns()
        {
            StructType schema = new StructType(new[] {new StructField("num", new DecimalType())});

            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {123.45m}),
                new GenericRow(new object[] {99}),
                new GenericRow(new object[] {678})
            };

            DataFrame df = _session.CreateDataFrame(elements, schema);
            Try<double> result = Minimum("num").Calculate(df).Value;

            result.IsSuccess.ShouldBeTrue();
            result.Get().ShouldBe(99.0);
        }
    }
}
