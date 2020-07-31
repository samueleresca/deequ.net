using xdeequ.Util;

namespace xdeequ.Analyzers
{
    internal interface IFilterableAnalyzer
    {
        Option<string> FilterCondition();
    }
}
