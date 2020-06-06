using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers
{
    public static class Inizializers
    {
        public static Size Size(Option<string> where)
        {
           return new Size(where);
        }
    }
}