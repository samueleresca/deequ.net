using System;
using System.Collections.Generic;
using deequ.Analyzers;
using deequ.Interop;
using deequ.Interop.Utils;
using deequ.Metrics;
using deequ.Util;
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
        public static Option<Func<M, V>> _valuePicker;
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
            JvmObjectReference metric = (JvmObjectReference) objectReferenceProvider
                .Reference
                .Invoke("calculate",  ((IJvmObjectReferenceProvider)data).Reference,
                    objectReferenceProvider.Reference.Invoke("calculate$default$2"),
                    objectReferenceProvider.Reference.Invoke("calculate$default$2"));

            return Evaluate(new Dictionary<string, JvmObjectReference>{ {Analyzer.ToString(), metric}});
        }

        public ConstraintResult Evaluate(MapJvm analysisResult)
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

            return metric
                .Select(PickValueAndAssert)
                .GetOrElse(new ConstraintResult(this, ConstraintStatus.Failure,
                MissingAnalysis)).Value;
        }

        public ConstraintResult Evaluate(Dictionary<string, JvmObjectReference> analysisResult) {

            Option<MetricJvm<M>> metric;

            try
            {
                AnalyzerJvmBase jvmAnalyzer = ((IJvmObjectReferenceProvider)Analyzer).Reference;
                metric = new MetricJvm<M>(analysisResult[jvmAnalyzer.ToString()]);
            }
            catch (Exception e)
            {
                metric = Option<MetricJvm<M>>.None;
            }

            return metric
                .Select(PickValueAndAssert)
                .GetOrElse(new ConstraintResult(this, ConstraintStatus.Failure,
                    MissingAnalysis)).Value;

        }

        private Option<ConstraintResult> PickValueAndAssert(MetricJvm<M> metric)
        {
            try
            {
                if (!metric.IsSuccess)
                {
                    ExceptionJvm exceptionJvm = (JvmObjectReference) metric.Exception().Get();
                    return new ConstraintResult(this, ConstraintStatus.Failure, exceptionJvm.GetMessage(), metric);
                }

                V assertOn;

                assertOn = RunPickerOnMetric(metric.Value.Get());


                bool assertionOk = RunAssertion(assertOn);

                if (assertionOk)
                {
                    return new ConstraintResult (this, ConstraintStatus.Success, Option<string>.None,  metric);
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

        private V RunPickerOnMetric(object metricValue)
        {
            Option<V> result;

            try
            {
                if (!_valuePicker.HasValue)
                {
                    if (typeof(M) == typeof(DistributionJvm))
                    {
                        V value = (V) Activator.CreateInstance(typeof(V), (JvmObjectReference)metricValue);
                        result = value;

                        return result.Value;
                    }

                    result =  (V) metricValue;
                    return result.Value;
                }


                if (typeof(M) == typeof(DistributionJvm))
                {
                    M value = (M) Activator.CreateInstance(typeof(M), (JvmObjectReference)metricValue);
                    result = _valuePicker.Value(value);
                }
                else
                {
                    result = _valuePicker.Value((M)metricValue);
                }
            }
            catch (Exception e)
            {
                throw new ValuePickerException(e.Message);
            }

            return result.Value;
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
