using xdeequ.Analyzers.Runners;

namespace xdeequ.Repository
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
