using System.Collections.Generic;
using deequ.Analyzers;
using deequ.Metrics;
using deequ.tests.Analyzers;
using Microsoft.Spark.Sql;
using Shouldly;
using Xunit;
using static Microsoft.Spark.Sql.Functions;
using static deequ.Analyzers.Initializers;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class DataTypeAnalyzer
    {
        public DataTypeAnalyzer(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        private Distribution GetDefaultDistribution() =>
            new Distribution(
                new Dictionary<string, DistributionValue>
                {
                    {DataTypeInstances.Unknown.ToString(), new DistributionValue(0, 0)},
                    {DataTypeInstances.Fractional.ToString(), new DistributionValue(0, 0)},
                    {DataTypeInstances.Integral.ToString(), new DistributionValue(0, 0)},
                    {DataTypeInstances.Boolean.ToString(), new DistributionValue(0, 0)},
                    {DataTypeInstances.String.ToString(), new DistributionValue(0, 0)}
                }, 5);

        [Fact]
        public void detect_factorial_type_in_string_correctly()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session)
                .WithColumn("att1_float", Column("att1").Cast("float"));

            HistogramMetric result = DataType("att1_float").Calculate(df);

            Distribution expected1 = GetDefaultDistribution();
            expected1[DataTypeInstances.Fractional.ToString()] = new DistributionValue(6, 1.0);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get()[DataTypeInstances.Fractional.ToString()].Absolute
                .ShouldBe(expected1[DataTypeInstances.Fractional.ToString()].Absolute);
            result.Value.Get()[DataTypeInstances.Fractional.ToString()].Ratio
                .ShouldBe(expected1[DataTypeInstances.Fractional.ToString()].Ratio);
        }

        [Fact]
        public void datatype_correctly_tostring_instances()
        {
            DataType("att1_float").ToString().ShouldBe("DataType(att1_float,None)");
        }

        [Fact]
        public void detect_fractional_type_correctly()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session)
                .WithColumn("att1_float", Column("att1").Cast("float"));

            HistogramMetric result = DataType("att1_float").Calculate(df);
            Distribution expected1 = GetDefaultDistribution();

            expected1[DataTypeInstances.Fractional.ToString()] = new DistributionValue(6, 1.0);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get()[DataTypeInstances.Fractional.ToString()].Absolute
                .ShouldBe(expected1[DataTypeInstances.Fractional.ToString()].Absolute);
            result.Value.Get()[DataTypeInstances.Fractional.ToString()].Ratio
                .ShouldBe(expected1[DataTypeInstances.Fractional.ToString()].Ratio);
        }

        [Fact]
        public void detect_fractional_type_correctly_for_negative_numbers()
        {
            DataFrame df = FixtureSupport.GetDFWithNegativeNumbers(_session);

            HistogramMetric result = DataType("att2").Calculate(df);
            Distribution expected1 = GetDefaultDistribution();

            expected1[DataTypeInstances.Fractional.ToString()] = new DistributionValue(4, 1.0);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get()[DataTypeInstances.Fractional.ToString()].Absolute
                .ShouldBe(expected1[DataTypeInstances.Fractional.ToString()].Absolute);
            result.Value.Get()[DataTypeInstances.Fractional.ToString()].Ratio
                .ShouldBe(expected1[DataTypeInstances.Fractional.ToString()].Ratio);
        }

        [Fact]
        public void detect_integral_type_correctly()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session);

            HistogramMetric result = DataType("att1").Calculate(df);
            Distribution expected1 = GetDefaultDistribution();

            expected1[DataTypeInstances.Integral.ToString()] = new DistributionValue(6, 1.0);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get()[DataTypeInstances.Integral.ToString()].Absolute
                .ShouldBe(expected1[DataTypeInstances.Integral.ToString()].Absolute);
            result.Value.Get()[DataTypeInstances.Integral.ToString()].Ratio
                .ShouldBe(expected1[DataTypeInstances.Integral.ToString()].Ratio);
        }

        [Fact]
        public void detect_integral_type_correctly_for_negative_numbers()
        {
            DataFrame df = FixtureSupport.GetDFWithNegativeNumbers(_session);

            HistogramMetric result = DataType("att1").Calculate(df);
            Distribution expected1 = GetDefaultDistribution();

            expected1[DataTypeInstances.Integral.ToString()] = new DistributionValue(4, 1.0);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get()[DataTypeInstances.Integral.ToString()].Absolute
                .ShouldBe(expected1[DataTypeInstances.Integral.ToString()].Absolute);
            result.Value.Get()[DataTypeInstances.Integral.ToString()].Ratio
                .ShouldBe(expected1[DataTypeInstances.Integral.ToString()].Ratio);
        }

        [Fact]
        public void detect_integral_type_in_string_correctly()
        {
            DataFrame df = FixtureSupport.GetDfWithNumericValues(_session)
                .WithColumn("att1_str", Column("att1").Cast("string"));

            HistogramMetric result = DataType("att1_str").Calculate(df);

            Distribution expected1 = GetDefaultDistribution();
            expected1[DataTypeInstances.Integral.ToString()] = new DistributionValue(6, 1.0);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get()[DataTypeInstances.Integral.ToString()].Absolute
                .ShouldBe(expected1[DataTypeInstances.Integral.ToString()].Absolute);
            result.Value.Get()[DataTypeInstances.Integral.ToString()].Ratio
                .ShouldBe(expected1[DataTypeInstances.Integral.ToString()].Ratio);
        }

        [Fact]
        public void fail_for_non_atomic_columns()
        {
            DataFrame df = FixtureSupport.GetDfWithNestedColumn(_session);
            DataType("source").Calculate(df).Value.IsSuccess().ShouldBeFalse();
        }

        [Fact]
        public void fall_back_to_String_in_case_no_known_data_type_matched()
        {
            DataFrame df = FixtureSupport.GetDFFull(_session);

            HistogramMetric result = DataType("att1").Calculate(df);
            Distribution expected1 = GetDefaultDistribution();

            expected1[DataTypeInstances.String.ToString()] = new DistributionValue(4, 1.0);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get()[DataTypeInstances.String.ToString()].Absolute
                .ShouldBe(expected1[DataTypeInstances.String.ToString()].Absolute);
            result.Value.Get()[DataTypeInstances.String.ToString()].Ratio
                .ShouldBe(expected1[DataTypeInstances.String.ToString()].Ratio);
        }
    }
}
