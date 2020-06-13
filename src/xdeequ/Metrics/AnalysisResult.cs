using xdeequ.Analyzers.Runners;

namespace xdeequ.Metrics
{
    public class AnalysisResult
    {
        public ResultKey ResultKey;
        public AnalyzerContext AnalyzerContext;

        public AnalysisResult(ResultKey resultKey, AnalyzerContext analyzerContext)
        {
            ResultKey = resultKey;
            AnalyzerContext = analyzerContext;
        }
    }
}