using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Analyzers;
using deequ.Analyzers.Runners;
using deequ.Interop.Utils;
using deequ.Repository;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace deequ
{
    public class MetricsRepository : IJvmObjectReferenceProvider, IMetricsRepository
    {
        protected readonly JvmObjectReference _repository;

        public MetricsRepository(JvmObjectReference repository)
        {
            _repository = repository;
        }

        public void Save(ResultKey resultKey, AnalyzerContext analyzerContext) => throw new NotImplementedException();

        public Option<AnalyzerContext> LoadByKey(ResultKey resultKey) => throw new NotImplementedException();

        IMetricRepositoryMultipleResultsLoader IMetricsRepository.Load() => throw new NotImplementedException();

        public MetricsRepository Load()
        {
            _repository.Invoke("Load");
            return this;
        }

        public MetricsRepository WithTagValues(Dictionary<string, string> tagValues)
        {
            MapJvm mapJvm = new MapJvm(_repository.Jvm);
            mapJvm.PutAll(tagValues);

            _repository.Invoke("withTagValues", mapJvm.Reference);
            return this;
        }

        public MetricsRepository ForAnalyzers(IEnumerable<AnalyzerJvmBase> analyzers)
        {

            _repository.Invoke("forAnalyzers",
                new SeqJvm(_repository, analyzers.Select(x => x.Reference)).Reference);

            return this;
        }

        public MetricsRepository Before(int dateTime)
        {
            _repository.Invoke("before", dateTime);
            return this;
        }

        public MetricsRepository After(int dateTime)
        {
            _repository.Invoke("after", dateTime);
            return this;
        }

        public string GetSuccessMetricsAsJson(IEnumerable<string> withTags)
        {
           return (string) _repository.Invoke("getSuccessMetricsAsJson",
                new SeqJvm(_repository, withTags.ToArray()));
        }

        public DataFrame GetSuccessMetricsAsDataFrame(IEnumerable<string> withTags)
        {
            return (DataFrame) _repository.Invoke("getSuccessMetricsAsDataFrame",
                new SeqJvm(_repository, withTags.ToArray()));
        }

        public JvmObjectReference Reference => _repository;
    }

    public class InMemoryMetricsRepository : MetricsRepository
    {
        public InMemoryMetricsRepository() :
            base(SparkEnvironment.JvmBridge.CallConstructor("com.amazon.deequ.repository.memory.InMemoryMetricsRepository"))
        {

        }
    }

    public class FileSystemMetricsRepository : MetricsRepository
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

    public class ResultKeyJvm : IJvmObjectReferenceProvider
    {
        public readonly long DataSetDate;
        private readonly JvmObjectReference _jvmResultKey;


        public ResultKeyJvm(Option<long> dataSetDate, Dictionary<string, string> tags = default)
        {
            if (!dataSetDate.HasValue)
            {
                dataSetDate = DateTime.Now.Ticks;
            }

            DataSetDate = dataSetDate.Value;

            MapJvm tagsMapJvm = new MapJvm(SparkEnvironment.JvmBridge);
            tagsMapJvm.PutAll(tags);

            _jvmResultKey =
                SparkEnvironment.JvmBridge.CallConstructor("com.amazon.deequ.repository.ResultKey",
                    dataSetDate,
                    tagsMapJvm.Reference);
        }

        public JvmObjectReference Reference => _jvmResultKey;
    }


    public class ResultKey
    {
        public ResultKey(long dataSetDate, Dictionary<string, string> tags)
        {
            DataSetDate = dataSetDate;
            Tags = tags;
        }

        public ResultKey(long dataSetDate)
        {
            DataSetDate = dataSetDate;
            Tags = new Dictionary<string, string>();
        }

        public ResultKey()
        {
        }

        public long DataSetDate { get; set; }
        public Dictionary<string, string> Tags { get; set; }
    }

}
