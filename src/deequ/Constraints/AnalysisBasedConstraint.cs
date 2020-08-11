using System;
using System.Collections.Generic;
using deequ.Analyzers;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;

namespace deequ.Constraints
{
    internal class AnalysisBasedConstraint<M, V> : IAnalysisBasedConstraint
    {
        public string AssertionException = "Can't execute the assertion";
        public string MissingAnalysis = "Missing Analysis, can't run the constraint!";
        public string ProblematicMetricPicker = "Can't retrieve the value to assert on";

        private readonly Func<V, bool> _assertion;
        private Option<Func<M, V>> _valuePicker;
        private Option<string> _hint;


        public AnalysisBasedConstraint(IAnalyzer<IMetric> analyzer,
            Func<V, bool> assertion, Option<Func<M, V>> valuePicker, Option<string> hint)
        {
            Analyzer = analyzer;
            _assertion = assertion;
            _valuePicker = valuePicker;
            _hint = hint;
        }

        public AnalysisBasedConstraint(IAnalyzer<IMetric> analyzer,
            Func<V, bool> assertion, Option<string> hint)
        {
            Analyzer = analyzer;
            _assertion = assertion;
            _hint = hint;
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
                {
                    return new ConstraintResult(this, ConstraintStatus.Failure,
                        metric.Value.Failure.Value.Message, metric);
                }

                V assertOn = RunPickerOnMetric(metric.Value.Get());
                bool assertionOk = RunAssertion(assertOn);

                if (assertionOk)
                {
                    return new ConstraintResult(this, ConstraintStatus.Success, Option<string>.None, metric);
                }

                string errorMessage = $"Value: {assertOn} does not meet the constraint requirement!";
                errorMessage += _hint.GetOrElse(string.Empty);

                return new ConstraintResult(this, ConstraintStatus.Failure, errorMessage, metric);
            }
            catch (Exception e)
            {
                switch (e)
                {
                    case ConstraintAssertionException ec:
                        return new ConstraintResult(this, ConstraintStatus.Failure,
                            $"{AssertionException}: {ec.Message}", metric);
                    case ValuePickerException vpe:
                        return new ConstraintResult(this, ConstraintStatus.Failure,
                            $"{ProblematicMetricPicker}: {vpe.Message}", metric);
                }
            }

            return Option<ConstraintResult>.None;
        }

        private V RunPickerOnMetric(M metricValue)
        {
            try
            {
                Option<V> result;
                if (_valuePicker.HasValue)
                {
                    result = _valuePicker.Select(func => func(metricValue));
                }
                else
                {
                    result = (V)(object)metricValue;
                }

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
                return _assertion(assertOn);
            }
            catch (Exception e)
            {
                throw new ConstraintAssertionException(e.Message);
            }
        }

        public ConstraintResult CalculateAndEvaluate(DataFrame df)
        {
            Metric<M> metric = Analyzer.Calculate(df) as Metric<M>;
            return Evaluate(new Dictionary<IAnalyzer<IMetric>, IMetric> { { Analyzer, metric } });
        }
    }

    internal class ValuePickerException : Exception
    {
        public ValuePickerException(string message) : base(message)
        {
        }
    }

    internal class ConstraintAssertionException : Exception
    {
        public ConstraintAssertionException(string message) : base(message)
        {
        }
    }
}
