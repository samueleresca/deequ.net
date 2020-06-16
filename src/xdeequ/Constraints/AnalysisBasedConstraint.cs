using System;
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Constraints
{
    public class AnalysisBasedConstraint<S, M, V> : IAnalysisBasedConstraint
        where S : IState
    {
        private readonly Func<V, bool> assertion;
        public string AssertionException = "Can't execute the assertion";
        private Option<string> hint;

        public string MissingAnalysis = "Missing Analysis, can't run the constraint!";
        public string ProblematicMetricPicker = "Can't retrieve the value to assert on";
        private Option<Func<M, V>> valuePicker;

        public AnalysisBasedConstraint(IAnalyzer<IMetric> analyzer,
            Func<V, bool> assertion, Option<Func<M, V>> valuePicker, Option<string> hint)
        {
            Analyzer = analyzer;
            this.assertion = assertion;
            this.valuePicker = valuePicker;
            this.hint = hint;
        }

        public AnalysisBasedConstraint(IAnalyzer<IMetric> analyzer,
            Func<V, bool> assertion, Option<string> hint)
        {
            Analyzer = analyzer;
            this.assertion = assertion;
            this.hint = hint;
        }

        public IAnalyzer<IMetric> Analyzer { get; }

        public ConstraintResult Evaluate(Dictionary<IAnalyzer<IMetric>, IMetric> analysisResult)
        {
            Option<Metric<M>> metric;
            try
            {
                metric = analysisResult[Analyzer] as Metric<M>;
            }
            catch (Exception e)
            {
                metric = Option<Metric<M>>.None;
            }

            return metric.Select(PickValueAndAssert).GetOrElse(new ConstraintResult(this, ConstraintStatus.Failure,
                MissingAnalysis, Option<IMetric>.None)).Value;
        }

        private Option<ConstraintResult> PickValueAndAssert(Metric<M> metric)
        {
            try
            {
                if (!metric.Value.IsSuccess)
                    return new ConstraintResult(this, ConstraintStatus.Failure,
                        metric.Value.Failure.Value.Message, metric);

                var assertOn = RunPickerOnMetric(metric.Value.Get());
                var assertionOk = RunAssertion(assertOn);

                if (assertionOk)
                    return new ConstraintResult(this, ConstraintStatus.Success, Option<string>.None, metric);

                var errorMessage = $"Value: {assertOn.ToString()} does not meet the constraint requirement!";
                hint = hint.Select(err => err += hint);

                return new ConstraintResult(this, ConstraintStatus.Failure, errorMessage, metric);
            }
            catch (Exception e)
            {
                switch (e)
                {
                    case ConstraintAssertionException ec:
                        return new ConstraintResult(this, ConstraintStatus.Failure,
                            $"{AssertionException}: {ec.Message}!", metric);
                    case ValuePickerException vpe:
                        return new ConstraintResult(this, ConstraintStatus.Failure,
                            $"{ProblematicMetricPicker}: {vpe.Message}!", metric);
                }
            }

            return Option<ConstraintResult>.None;
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

        public ConstraintResult CalculateAndEvaluate(DataFrame df)
        {
            var metric = Analyzer.Calculate(df) as Metric<M>;
            return Evaluate(new Dictionary<IAnalyzer<IMetric>, IMetric> { { Analyzer, metric } });
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