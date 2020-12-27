using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace deequ.Metrics
{
    public class MetricsRepositoryBase
    {
        protected readonly JvmObjectReference _repository;

        public MetricsRepositoryBase(JvmObjectReference repository)
        {
            _repository = repository;
        }

        public MetricsRepositoryBase Load()
        {
            _repository.Invoke("Load");
            return this;
        }

        public MetricsRepositoryBase WithTagValues(Dictionary<string, string> tagValues)
        {
            Map map = new Map(_repository.Jvm);
            map.PutAll(tagValues);

            _repository.Invoke("withTagValues", map.Reference);
            return this;
        }

        public MetricsRepositoryBase ForAnalyzers(IEnumerable<AnalyzerJvmBase> analyzers)
        {

            foreach (AnalyzerJvmBase analyzer in analyzers)
                analyzer.JvmObjectReference = _repository;

            _repository.Invoke("forAnalyzers",
                new Seq<IEnumerable<JvmObjectReference>>(_repository, analyzers.Select(x => x.Reference)));

            return this;
        }

        public MetricsRepositoryBase Before(int dateTime)
        {
            _repository.Invoke("before", dateTime);
            return this;
        }

        public MetricsRepositoryBase After(int dateTime)
        {
            _repository.Invoke("after", dateTime);
            return this;
        }

        public string GetSuccessMetricsAsJson(IEnumerable<string> withTags)
        {
           return (string) _repository.Invoke("getSuccessMetricsAsJson",
                new Seq<string>(_repository, withTags.ToArray()));
        }

        public DataFrame GetSuccessMetricsAsDataFrame(IEnumerable<string> withTags)
        {
            return (DataFrame) _repository.Invoke("getSuccessMetricsAsDataFrame",
                new Seq<string>(_repository, withTags.ToArray()));
        }
    }

    public class InMemoryMetricsRepository : MetricsRepositoryBase
    {
        public InMemoryMetricsRepository() :
            base(SparkEnvironment.JvmBridge.CallConstructor("com.amazon.deequ.repository.memory.InMemoryMetricsRepository"))
        {

        }
    }

    public class FileSystemMetricsRepository : MetricsRepositoryBase
    {

        private readonly string _path;

        public FileSystemMetricsRepository(Option<string> path) :
            base(SparkEnvironment.JvmBridge.CallConstructor("com.amazon.deequ.repository.fs.FileSystemMetricsRepository"))
        {
            if (!path.HasValue)
            {
                path = MetricsFileHelper();
            }

            _path = path.Value;
        }

        private string MetricsFileHelper(string filename = "metrics.json")
        {
            var tempDir = _repository.Jvm.CallConstructor("com.google.common.io.Files.createTempDir");
            var file = _repository.Jvm.CallConstructor("java.io.File", tempDir, filename);
            string f_path = (string) file.Invoke("getAbsolutePath");
            return f_path;
        }
    }
}
