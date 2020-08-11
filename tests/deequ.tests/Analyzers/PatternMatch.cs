using System.Collections.Generic;
using System.Text.RegularExpressions;
using deequ.Analyzers;
using deequ.Metrics;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Shouldly;
using Xunit;
using static deequ.Analyzers.Initializers;

namespace xdeequ.tests.Analyzers
{
    [Collection("Spark instance")]
    public class PatternMatch
    {
        public PatternMatch(SparkFixture fixture) => _session = fixture.Spark;

        private readonly SparkSession _session;
        private readonly string someColumnName = "some";

        [Fact]
        public void match_credit_card_numbers()
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"378282246310005"}),
                new GenericRow(new object[] {"6011111111111117"}),
                new GenericRow(new object[] {"6011 1111 1111 1117"}),
                new GenericRow(new object[] {"6011-1111-1111-1117"}),
                new GenericRow(new object[] {"4111111111111111"}),
                new GenericRow(new object[] {"4111 1111 1111 1111"}),
                new GenericRow(new object[] {"4111-1111-1111-1111"}),
                new GenericRow(new object[] {"0000111122223333"}),
                new GenericRow(new object[] {"000011112222333"}),
                new GenericRow(new object[] {"00001111222233"})
            };

            StructType schema = new StructType(
                new List<StructField> { new StructField(someColumnName, new StringType()) });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            DoubleMetric result = PatternMatch(someColumnName, Patterns.CreditCard)
                .Calculate(df);

            result.Value.IsSuccess.ShouldBeTrue();
            result.Value.Get().ShouldBe(7.0 / 10.0);
        }

        [Fact]
        public void match_email_addresses()
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"someone@somewhere.org"}),
                new GenericRow(new object[] {"someone@else"})
            };

            StructType schema = new StructType(
                new List<StructField> { new StructField(someColumnName, new StringType()) });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            DoubleMetric result = PatternMatch(someColumnName, Patterns.Email)
                .Calculate(df);

            result.Value.IsSuccess.ShouldBeTrue();
            result.Value.Get().ShouldBe(0.5);
        }

        [Fact]
        public void match_integers_in_a_String_column()
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"1"}), new GenericRow(new object[] {"a"})
            };

            StructType schema = new StructType(
                new List<StructField> { new StructField(someColumnName, new StringType()) });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            DoubleMetric result = PatternMatch(someColumnName, new Regex(@"\d"))
                .Calculate(df);

            result.Value.IsSuccess.ShouldBeTrue();
            result.Value.Get().ShouldBe(0.5);
        }

        [Fact]
        public void patternmatch_correctly_tostring_instances()
        {
            PatternMatch("att1", new Regex("")).ToString().ShouldBe("PatternMatch(att1,None)");
        }

        [Fact]
        public void match_URLS()
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"http://foo.com/blah_blah"}),
                new GenericRow(new object[] {"http://foo.com/blah_blah_(wikipedia)"}),
                new GenericRow(new object[] {"http://foo.com/blah_blah_(wikipedia)"}),
                new GenericRow(new object[] {"http://➡.ws/䨹"}),
                new GenericRow(new object[] {"http://⌘.ws/"}),
                new GenericRow(new object[] {"http://☺.damowmow.com/"}),
                new GenericRow(new object[] {"http://例子.测试"}),
                new GenericRow(new object[] {"https://foo_bar.example.com/"}),
                new GenericRow(new object[] {"http://userid@example.com:8080"}),
                new GenericRow(new object[] {"http://foo.com/blah_(wikipedia)#cite-1"}),
                new GenericRow(new object[] {"http://../"}),
                new GenericRow(new object[] {"h://test"}),
                new GenericRow(new object[] {"http://.www.foo.bar/"})
            };

            StructType schema = new StructType(
                new List<StructField> { new StructField(someColumnName, new StringType()) });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            DoubleMetric result = PatternMatch(someColumnName, Patterns.Url)
                .Calculate(df);

            result.Value.IsSuccess.ShouldBeTrue();
            result.Value.Get().ShouldBe(10.0 / 13.0);
        }

        [Fact]
        public void match_US_SSN()
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {"111-05-1130"}),
                new GenericRow(new object[] {"111051130"}),
                new GenericRow(new object[] {"111-05-000"}),
                new GenericRow(new object[] {"111-00-000"}),
                new GenericRow(new object[] {"000-05-1130"}),
                new GenericRow(new object[] {"666-05-1130"}),
                new GenericRow(new object[] {"900-05-1130"}),
                new GenericRow(new object[] {"999-05-1130"})
            };

            StructType schema = new StructType(
                new List<StructField> { new StructField(someColumnName, new StringType()) });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            DoubleMetric result = PatternMatch(someColumnName, Patterns.SocialSecurityNumberUs)
                .Calculate(df);

            result.Value.IsSuccess.ShouldBeTrue();
            result.Value.Get().ShouldBe(2.0 / 8.0);
        }

        [Fact]
        public void not_match_doubles_in_nullable_column()
        {
            List<GenericRow> elements = new List<GenericRow>
            {
                new GenericRow(new object[] {1.1}),
                new GenericRow(new object[] {null}),
                new GenericRow(new object[] {3.2}),
                new GenericRow(new object[] {4.4})
            };

            StructType schema = new StructType(
                new List<StructField> { new StructField(someColumnName, new DoubleType()) });

            DataFrame df = _session.CreateDataFrame(elements, schema);

            DoubleMetric result = PatternMatch(someColumnName, new Regex(@"\d\.\d"))
                .Calculate(df);

            result.Value.IsSuccess.ShouldBeFalse();
        }
    }
}
