using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Shouldly;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Repository;
using xdeequ.Repository.InMemory;
using xdeequ.Util;
using Xunit;

namespace xdeequ.tests.Repository
{
    [Collection("Spark instance")]
    public class MetricsRepositoryMultipleResultsLoaderTest
    {
        private static readonly long DATE_ONE = new DateTime(2021, 10, 14).ToBinary();
        private static readonly long DATE_TWO = new DateTime(2021, 10, 15).ToBinary();

        private static readonly KeyValuePair<string, string>[] REGION_EU =
        {
            new KeyValuePair<string, string>("Region", "EU")
        };

        private static readonly KeyValuePair<string, string>[] REGION_NA =
        {
            new KeyValuePair<string, string>("Region", "NA")
        };

        private static readonly KeyValuePair<string, string>[] REGION_EU_AND_DATASET_NAME =
        {
            new KeyValuePair<string, string>("Region", "EU"),
            new KeyValuePair<string, string>("dataset_name", "Some")
        };

        private static readonly KeyValuePair<string, string>[] REGION_NA_AND_DATASET_VERSION =
        {
            new KeyValuePair<string, string>("Region", "EU"),
            new KeyValuePair<string, string>("dataset_version", "2.0")
        };

        private readonly SparkSession _session;

        public MetricsRepositoryMultipleResultsLoaderTest(SparkFixture fixture) => _session = fixture.Spark;

        private static void Evaluate(SparkSession session, Action<AnalyzerContext, IMetricsRepository> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            AnalyzerContext results = CreateAnalysis()
                .Run(data, Option<IStateLoader>.None, Option<IStatePersister>.None, new StorageLevel());

            InMemoryMetricsRepository repository = new InMemoryMetricsRepository();

            func(results, repository);
        }

        private static Analysis CreateAnalysis() =>
            new Analysis()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] {"item"}, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att1"))
                .AddAnalyzer(Initializers.Uniqueness(new[] {"att1", "att2"}));

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
    }
}
