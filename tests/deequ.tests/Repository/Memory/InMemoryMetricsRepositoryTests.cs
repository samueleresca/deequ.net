using System;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Repository;
using xdeequ.Util;

namespace xdeequ.tests.Repository.Memory
{
    public class InMemoryMetricsRepositoryTests
    {
        private static void Evaluate(SparkSession session, Action<AnalyzerContext> func)
        {
            DataFrame data = FixtureSupport.GetDFFull(session);

            AnalyzerContext results = CreateAnalysis().Run(data, Option<IStateLoader>.None,
                Option<IStatePersister>.None,
                new StorageLevel());

            func(results);
        }

        private static Analysis CreateAnalysis() =>
            new Analysis()
                .AddAnalyzer(Initializers.Size(Option<string>.None))
                .AddAnalyzer(Initializers.Distinctness(new[] {"item"}, Option<string>.None))
                .AddAnalyzer(Initializers.Completeness("att1"))
                .AddAnalyzer(Initializers.Uniqueness(new[] {"att1", "att2"}));

        private static long CreateDate(int year, int month, int day) => new DateTime(year, month, day).ToBinary();

        private static IMetricsRepository CreateRepository() => new InMemoryMetricsRepository();
    }
}
