using System;
using System.Collections.Generic;
using System.Data;
using xdeequ.Analyzers;
using static xdeequ.Analyzers.Inizializers;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Constraints
{
    public abstract class Constraint<S, M, V> where S : State<S>
    {
        public abstract ConstraintResult<S, M, V> Evaluate(Dictionary<Analyzer<S, Metric<M>>, Metric<M>> analysisResult);
    }

    public class ConstraintResult<S, M, V> where S : State<S>
    {
        public Constraint<S, M, V> Constraint { get; set; }
        public ConstraintStatus Status { get; set; }
        public Option<string> Message { get; set; } = new Option<string>();
        
        public Option<Metric<M>> Metric = new Option<Metric<M>>();


        public ConstraintResult(Constraint<S, M, V> constraint, ConstraintStatus status, Option<string> message,
            Option<Metric<M>> metric)
        {
            Constraint = constraint;
            Status = status;
            Message = message;
            Metric = metric;
        }
    }

    public enum ConstraintStatus
    {
         Success = 0,
         Error = 1
    }


    public class ConstraintDecorator<S, M, V> : Constraint<S, M, V> where S : State<S>
    {
        public Constraint<S, M, V> Inner;



        public override ConstraintResult<S, M, V> Evaluate(Dictionary<Analyzer<S, Metric<M>>, Metric<M>> analysisResult)
        {
            return Inner.Evaluate(analysisResult);
        }
    }
    
    /**
  * Constraint decorator which holds a name of the constraint along with it
  *
  * @param constraint Delegate
  * @param name       Name (Detailed message) for the constraint
  */
    public class NamedConstraint<S,M,V> : ConstraintDecorator<S,M,V> where S : State<S> 
    {
        private string _name { get; set; }
        private Constraint<S, M, V> _inner;

        public NamedConstraint(Constraint<S, M, V> inner, string name )
        {
            _name = name;
            _inner = inner;
        }

        public override string ToString()
        {
            return _name;
        }
    }
    
    public static class Constraint{

        
        public static Constraint<NumMatches, double, long> SizeConstraint(Func<long, bool> assertion, Option<string> where, Option<string> hint)
        {
            Analyzer<NumMatches, Metric<double>> size =  (Analyzer<NumMatches, Metric<double>>) (object) Size(@where);
            //AnalysisBasedConstraint<NumMatches, double, long> constraint =  new AnalysisBasedConstraint<NumMatches, double, long>(size, assertion, null, hint);

            return new NamedConstraint<NumMatches, double, long>(null, $"SizeConstraint{size}");
        }
    }
}