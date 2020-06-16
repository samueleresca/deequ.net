using System;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Constraints;
using xdeequ.Metrics;
using xdeequ.Util;
using Xunit;
using static xdeequ.Constraints.Functions;

namespace xdeequ.tests.Constraints
{
    [Collection("Spark instance")]
    public class HistogramConstraints
    {
        public HistogramConstraints(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }

        private readonly SparkSession _session;

        [Fact]
        public void assert_on_bin_number()
        {
            var df = FixtureSupport.GetDFMissing(_session);

            ConstraintUtils.Calculate<FrequenciesAndNumRows, Distribution, long>(HistogramBinConstraint("att1",
                    d => d == 3,
                    Option<Func<Column, Column>>.None, Option<string>.None, Option<string>.None), df)
                .Status.ShouldBe(ConstraintStatus.Success);
            ConstraintUtils.Calculate<FrequenciesAndNumRows, Distribution, long>(HistogramBinConstraint("att1",
                    d => d != 3,
                    Option<Func<Column, Column>>.None, Option<string>.None, Option<string>.None), df)
                .Status.ShouldBe(ConstraintStatus.Failure);
        }

        [Fact]
        public void assert_on_ratios_for_a_column_value_which_does_not_exist()
        {
            var df = FixtureSupport.GetDFMissing(_session);

            var metric = ConstraintUtils.Calculate<FrequenciesAndNumRows, Distribution, Distribution>(
                HistogramConstraint("att1",
                    _ => _["non-existent-column-value"].Ratio == 3, Option<Func<Column, Column>>.None,
                    Option<string>.None,
                    Option<string>.None), df);

            metric.Status.ShouldBe(ConstraintStatus.Failure);
            metric.Message.ShouldNotBeNull();
            metric.Message.Value.StartsWith("Can't execute the assertion").ShouldBeTrue();
        }
    }
}