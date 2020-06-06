using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Xml.Schema;
using xdeequ.Analyzers;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Constraints
{
    public class AnalysisBasedConstraint<S, M, V> : Constraint<S, M, V>
        where S : State<S>
        where M : Metric<M>
    {
        public Analyzer<S, Metric<M>> Analyzer;
        private Func<V, bool> assertion;
        private Option<Func<M, V>> valuePicker;
        private Option<string> hint;

        private string MissingAnalysis = "Missing Analysis, can't run the constraint!";
        private string ProblematicMetricPicker = "Can't retrieve the value to assert on";
        private string AssertionException = "Can't execute the assertion";

        public AnalysisBasedConstraint(Analyzer<S, Metric<M>> analyzer,
            Func<V, bool> assertion, Option<Func<M, V>> valuePicker, Option<string> hint)
        {
            Analyzer = analyzer;
            this.assertion = assertion;
            this.valuePicker = valuePicker;
            this.hint = hint;
        }

        private Option<ConstraintResult<S, M, V>> PickValueAndAssert(Metric<M> metric)
        {
            if (!metric.Value.IsSuccess)
            {
                return new ConstraintResult<S,M,V>(this, ConstraintStatus.Error,
                    new Option<string>(metric.Value.Failure.Value.Message), new Option<Metric<M>>(metric));
            }

            try
            {
                var assertOn = RunPickerOnMetric(metric.Value.Get());
                var assertionOk = RunAssertion(assertOn);

                if (assertionOk)
                {
                    return new ConstraintResult<S,M,V>(this, ConstraintStatus.Success,
                        new Option<string>(), new Option<Metric<M>>(metric));
                }

                var errorMessage = $"Value: {assertOn} does not meet the constraint requirement!";
                hint = hint.Select(err => err += hint);

                return new ConstraintResult<S,M,V>(this, ConstraintStatus.Error, new Option<string>(errorMessage),
                    new Option<Metric<M>>(metric));
            }
            catch (Exception e)
            {
                if (e is ConstraintAssertionException)
                {
                    return new ConstraintResult<S,M,V>(this, ConstraintStatus.Error,
                        new Option<string>($"{AssertionException}: $msg!"),
                        new Option<Metric<M>>(metric));
                }

                if (e is ValuePickerException)
                {
                    return new ConstraintResult<S,M,V>(this, ConstraintStatus.Error,
                        new Option<string>($"{ProblematicMetricPicker}: $msg!"),
                        new Option<Metric<M>>(metric));
                }
            }

            return new Option<ConstraintResult<S, M, V>>();
        }

        private V RunPickerOnMetric(M metricValue)
        {
            try
            {
                var result = valuePicker.Select(func => func(metricValue));
                return result.Value;
            }
            catch (Exception e)
            {
                throw new ValuePickerException(e.Message);
            }
        }
        
        private bool RunAssertion(V assertOn)
        {
            try
            {
                return assertion(assertOn);
            }
            catch (Exception e)
            {
                throw new ConstraintAssertionException(e.Message);
            }
        }

        public override ConstraintResult<S,M,V> Evaluate(Dictionary<Analyzer<S, Metric<M>>, Metric<M>> analysisResult)
        {
            var metric = analysisResult[Analyzer];

            return PickValueAndAssert(metric).GetOrElse(new ConstraintResult<S, M, V>(this, ConstraintStatus.Error,
                    new Option<string>(MissingAnalysis), metric));
        }
    }
    
    public class ValuePickerException : Exception
    {
        private string _message;
        
        public ValuePickerException(string message) : base(message)
        {
            _message = message;
        }
    }
    
    public class ConstraintAssertionException : Exception
    {
        private string _message;
        
        public ConstraintAssertionException(string message) : base(message)
        {
            _message = message;
        }
    }
}