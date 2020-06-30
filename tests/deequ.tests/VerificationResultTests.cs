using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Checks;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.tests
{
    public class ValidationResultTests
    {




        private static void Evaluate(SparkSession session, Action<VerificationResult> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            IEnumerable<IAnalyzer<IMetric>> analyzers = GetAnalyzers();
            IEnumerable<Check> checks = GetChecks();

            VerificationResult results = new VerificationSuite()
                .OnData(data)
                .AddRequiredAnalyzer(analyzers)
                .AddChecks(checks)
                .Run();

            func(results);
        }

        private static IEnumerable<Check> GetChecks()
        {
            CheckWithLastConstraintFilterable checkToSucceed = new Check(CheckLevel.Error, "group-1")
                .IsComplete("att1", Option<string>.None);

            CheckWithLastConstraintFilterable checkToErrorOut = new Check(CheckLevel.Error, "group-2-E")
                .HasSize(_ => _ > 5, "Should be greater than 5!")
                .HasCompleteness("att2", _ => _ == 1.0, "Should equal 1!");

            CheckWithLastConstraintFilterable checkToWarn = new Check(CheckLevel.Warning, "group-2-W")
                .HasDistinctness(new[] {"item"}, _ => _ < 0.8, "Should be smaller than 0.8!");


            return new[] {checkToSucceed, checkToErrorOut, checkToWarn};
        }

        private static IEnumerable<IAnalyzer<IMetric>> GetAnalyzers() =>
            new[] {Initializers.Uniqueness(new[] {"att1", "att2"})};

        private static void AssertSameRows(DataFrame dataFrameA, DataFrame dataFrameB)
        {
            IEnumerable<Row> dfASeq = dataFrameA.Collect();
            IEnumerable<Row> dfBSeq = dataFrameB.Collect();

            int i = 0;
            foreach (Row rowA in dfASeq)
            {
                Row rowB = dfBSeq.Skip(i).First();

                rowA[0].ShouldBe(rowB[0]);
                rowA[1].ShouldBe(rowB[1]);
                rowA[2].ShouldBe(rowB[2]);
                rowA[3].ShouldBe(rowB[3]);
                rowA[4].ShouldBe(rowB[4]);
                rowA[5].ShouldBe(rowB[5]);

                i++;
            }
        }

        private static void AssertSameRows(string jsonA, string jsonB)
        {
            SimpleMetricOutput[] resultA =
                JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonA, SerdeExt.GetDefaultOptions());
            SimpleMetricOutput[] resultB =
                JsonSerializer.Deserialize<SimpleMetricOutput[]>(jsonB, SerdeExt.GetDefaultOptions());
            int i = 0;

            foreach (SimpleMetricOutput rowA in resultA)
            {
                SimpleMetricOutput rowB = resultB.Skip(i).First();

                rowA.Entity.ShouldBe(rowB.Entity);
                rowA.Instance.ShouldBe(rowB.Instance);
                rowA.Name.ShouldBe(rowB.Name);
                rowA.Value.ShouldBe(rowB.Value);

                i++;
            }
        }
    }
}
