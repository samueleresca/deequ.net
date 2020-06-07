using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Constraints
{
    public class AnalysisBasedConstraint<S, M, V> : IConstraint<S, M, V>
        where S : State<S>
    {
        public IAnalyzer<S, Metric<M>> Analyzer;
        private Func<V, bool> assertion;
        private Option<Func<M, V>> valuePicker;
        private Option<string> hint;

        public string MissingAnalysis = "Missing Analysis, can't run the constraint!";
        public string ProblematicMetricPicker = "Can't retrieve the value to assert on";
        public string AssertionException = "Can't execute the assertion";

        public AnalysisBasedConstraint(IAnalyzer<S, Metric<M>> analyzer,
            Func<V, bool> assertion, Option<Func<M, V>> valuePicker, Option<string> hint)
        {
            Analyzer = analyzer;
            this.assertion = assertion;
            this.valuePicker = valuePicker;
            this.hint = hint;
        }

        public AnalysisBasedConstraint(IAnalyzer<S, Metric<M>> analyzer,
            Func<V, bool> assertion, Option<string> hint)
        {
            Analyzer = analyzer;
            this.assertion = assertion;
            this.hint = hint;
        }

        private Option<ConstraintResult<S, M, V>> PickValueAndAssert(Metric<M> metric)
        {
            if (!metric.Value.IsSuccess)
            {
                return new ConstraintResult<S, M, V>(this, ConstraintStatus.Failure,
                    metric.Value.Failure.Value.Message, metric);
            }

            try
            {
                var assertOn = RunPickerOnMetric(metric.Value.Get());
                var assertionOk = RunAssertion(assertOn);

                if (assertionOk)
                {
                    return new ConstraintResult<S, M, V>(this, ConstraintStatus.Success, Option<string>.None, metric);
                }

                var errorMessage = $"Value: {assertOn} does not meet the constraint requirement!";
                hint = hint.Select(err => err += hint);

                return new ConstraintResult<S, M, V>(this, ConstraintStatus.Failure, errorMessage, metric);
            }
            catch (Exception e)
            {
                switch (e)
                {
                    case ConstraintAssertionException _:
                        return new ConstraintResult<S, M, V>(this, ConstraintStatus.Failure,
                            $"{AssertionException}: $msg!", metric);
                    case ValuePickerException _:
                        return new ConstraintResult<S, M, V>(this, ConstraintStatus.Failure,
                            $"{ProblematicMetricPicker}: $msg!", metric);
                }
            }

            return Option<ConstraintResult<S, M, V>>.None;
        }

        private V RunPickerOnMetric(M metricValue)
        {
            try
            {
                Option<V> result;
                if (valuePicker.HasValue) result = valuePicker.Select(func => func(metricValue));
                else result = (V)(object)metricValue;

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

        public ConstraintResult<S, M, V> CalculateAndEvaluate(DataFrame df)
        {
            var metric = Analyzer.Calculate(df);
            return Evaluate(new Dictionary<IAnalyzer<S, Metric<M>>, Metric<M>> { { Analyzer, metric } });
        }

        public override ConstraintResult<S, M, V> Evaluate(
            Dictionary<IAnalyzer<S, Metric<M>>, Metric<M>> analysisResult)
        {
            var metric = analysisResult[Analyzer];

            return PickValueAndAssert(metric).GetOrElse(new ConstraintResult<S, M, V>(this, ConstraintStatus.Failure,
                MissingAnalysis, metric));
        }
    }

    public class ValuePickerException : Exception
    {
        public ValuePickerException(string message) : base(message)
        {
        }
    }

    public class ConstraintAssertionException : Exception
    {
        public ConstraintAssertionException(string message) : base(message)
        {
        }
    }
}