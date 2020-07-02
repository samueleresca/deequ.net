using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Constraints;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using Functions = xdeequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    [Collection("Spark instance")]
    public class DataTypeConstraints
    {
        public DataTypeConstraints(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;

        [Fact]
        public void assert_fractional_type_for_DoubleType_column()
        {
            DataFrame df = FixtureSupport.DataFrameWithColumn("column", new DoubleType(), _session,
                new[] { new GenericRow(new object[] { 1.0 }), new GenericRow(new object[] { 2.0 }) });

            ConstraintUtils.Calculate<DataTypeHistogram, Distribution, double>(
                Functions.DataTypeConstraint("column", ConstrainableDataTypes.Fractional, _ => _ == 1.0,
                    Option<string>.None, Option<string>.None), df).Status.ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void assert_fractional_type_for_StringType_column()
        {
            DataFrame df = FixtureSupport.DataFrameWithColumn("column", new StringType(), _session,
                new[] { new GenericRow(new object[] { "1" }), new GenericRow(new object[] { "2.0" }) });

            ConstraintUtils.Calculate<DataTypeHistogram, Distribution, double>(
                Functions.DataTypeConstraint("column", ConstrainableDataTypes.Fractional, _ => _ == 0.5,
                    Option<string>.None, Option<string>.None), df).Status.ShouldBe(ConstraintStatus.Success);
        }

        [Fact]
        public void assert_numeric_type_as_sum_over_fractional_and_integral()
        {
            DataFrame df = FixtureSupport.DataFrameWithColumn("column", new StringType(), _session,
                new[] { new GenericRow(new object[] { "1" }), new GenericRow(new object[] { "2.0" }) });

            ConstraintUtils.Calculate<DataTypeHistogram, Distribution, double>(
                Functions.DataTypeConstraint("column", ConstrainableDataTypes.Numeric, _ => _ == 1.0,
                    Option<string>.None, Option<string>.None), df).Status.ShouldBe(ConstraintStatus.Success);
        }
    }
}
