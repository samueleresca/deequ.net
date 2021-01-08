using System;
using deequ.Analyzers;
using deequ.Interop;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace deequ.Constraints
{
    public class AnalysisBasedConstraint<M, V> : IAnalysisBasedConstraint
    {
        public string AssertionException = "Can't execute the assertion";
        public string MissingAnalysis = "Missing Analysis, can't run the constraint!";
        public string ProblematicMetricPicker = "Can't retrieve the value to assert on";


        private Func<V, bool> _assertion;
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

        public AnalysisBasedConstraint(IAnalyzer<IMetric> analyzer, Option<string> hint)
        {
            Analyzer = analyzer;
            _hint = hint;
        }

        public IAnalyzer<IMetric> Analyzer { get; }

        public ConstraintResult CalculateAndEvaluate(DataFrame data)
        {
            IJvmObjectReferenceProvider objectReferenceProvider = (IJvmObjectReferenceProvider)Analyzer;
            JvmObjectReference metric = (JvmObjectReference) objectReferenceProvider.Reference.Invoke("calculate", data);

            var map = new Map(SparkEnvironment.JvmBridge);
            map.Put(Analyzer, metric);

            return Evaluate(map);
        }

        public ConstraintResult Evaluate(Map analysisResult)
        {
            Option<MetricJvm<M>> metric;
            try
            {
                JvmObjectReference jvmAnalyzer = ((IJvmObjectReferenceProvider)Analyzer).Reference;
                OptionJvm option = analysisResult.Get(jvmAnalyzer);

                metric = new MetricJvm<M>((JvmObjectReference)option.Get());
            }
            catch (Exception e)
            {
                metric = Option<MetricJvm<M>>.None;
            }

            return metric.Select(PickValueAndAssert).GetOrElse(new ConstraintResult(this, ConstraintStatus.Failure,
                MissingAnalysis, Option<IMetric>.None)).Value;
        }

        private Option<ConstraintResult> PickValueAndAssert(MetricJvm<M> metric)
        {
            try
            {
                if (!metric.IsSuccess())
                {
                    return new ConstraintResult(this, ConstraintStatus.Failure,
                        metric.Exception().Value.Message, metric);
                }

                V assertOn = RunPickerOnMetric((M)metric.Value().Get());
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
