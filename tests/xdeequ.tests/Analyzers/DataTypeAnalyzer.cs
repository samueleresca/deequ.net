using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Metrics;
using Xunit;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class DataTypeAnalyzer
    {
        private readonly SparkSession _session;

        public static Distribution DistributionFrom(Dictionary<string, DistributionValue> keyValue)
        {
            return new Distribution(keyValue, keyValue.Count);
        }

        public DataTypeAnalyzer(SparkFixture fixture)
        {
            _session = fixture.Spark;
        }


        [Fact]
        public void fail_for_non_atomic_columns()
        {
            var df = FixtureSupport.GetDfWithNestedColumn(_session);
            DataType.Create("source").Calculate(df).Value.IsSuccess.ShouldBeFalse();
        }


        [Fact(Skip = "Review stateful data type")]
        public void fall_back_to_String_in_case_no_known_data_type_matched()
        {
            var df = FixtureSupport.GetDFFull(_session);

            var result = DataType.Create("att1").Calculate(df);
            var expected1 = new Distribution(new Dictionary<string, DistributionValue>
            {
                {DataTypeInstances.String.ToString(), new DistributionValue(6, 1.0)}
            }, 1);

            result.Value.Get().NumberOfBins.ShouldBe(expected1.NumberOfBins);
            result.Value.Get().Values.Values.First().Absolute.ShouldBe(expected1.Values.First().Value.Absolute);
            result.Value.Get().Values.Values.First().Ratio.ShouldBe(expected1.Values.First().Value.Ratio);
        }
    }
}