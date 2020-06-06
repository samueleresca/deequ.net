using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public interface IFilterableAnalyzer
    {
        Option<string> FilterCondition();
    }
}