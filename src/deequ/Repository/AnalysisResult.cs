using deequ.Analyzers.Runners;

namespace deequ.Repository
{
    public class AnalysisResult
    {
        public AnalyzerContext AnalyzerContext;
        public ResultKey ResultKey;

        public AnalysisResult(ResultKey resultKey, AnalyzerContext analyzerContext)
        {
            ResultKey = resultKey;
            AnalyzerContext = analyzerContext;
        }
    }
}
