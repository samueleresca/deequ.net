using deequ.Util;

namespace deequ.Analyzers
{
    internal interface IFilterableAnalyzer
    {
        Option<string> FilterCondition();
    }
}
