using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using static xdeequ.Analyzers.Initializers;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Constraints
{
    public abstract class IConstraint<S, M, V> where S : State<S>
    {
        public abstract ConstraintResult<S, M, V> Evaluate(
            Dictionary<IAnalyzer<S, Metric<M>>, Metric<M>> analysisResult);
    }

    public class ConstraintResult<S, M, V> where S : State<S>
    {
        public IConstraint<S, M, V> Constraint { get; set; }
        public ConstraintStatus Status { get; set; }
        public Option<string> Message { get; set; }

        public Option<Metric<M>> Metric = new Option<Metric<M>>();


        public ConstraintResult(IConstraint<S, M, V> constraint, ConstraintStatus status, Option<string> message,
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
        Failure = 1
    }


    public class ConstraintDecorator<S, M, V> : IConstraint<S, M, V> where S : State<S>
    {
        private IConstraint<S, M, V> _inner;

        public IConstraint<S, M, V> Inner
        {
            get
            {
                var dc = _inner is ConstraintDecorator<S, M, V>;
                if (!dc) return _inner;
                var result = (ConstraintDecorator<S, M, V>)_inner;
                return result.Inner;
            }
        }

        public ConstraintDecorator(IConstraint<S, M, V> constraint)
        {
            _inner = constraint;
        }

        public override ConstraintResult<S, M, V> Evaluate(
            Dictionary<IAnalyzer<S, Metric<M>>, Metric<M>> analysisResult)
        {
            return _inner.Evaluate(analysisResult);
        }
    }

    /**
  * Constraint decorator which holds a name of the constraint along with it
  *
  * @param constraint Delegate
  * @param name       Name (Detailed message) for the constraint
  */
    public class NamedConstraint<S, M, V> : ConstraintDecorator<S, M, V> where S : State<S>
    {
        private string _name { get; set; }

        public NamedConstraint(IConstraint<S, M, V> constraint, string name) : base(constraint)
        {
            _name = name;
        }

        public override string ToString() => _name;
    }

    public static class Functions
    {
        public static IConstraint<NumMatches, double, long> SizeConstraint(Func<long, bool> assertion,
            Option<string> where, Option<string> hint)
        {
            IAnalyzer<NumMatches, Metric<double>> size = Size(where);
            AnalysisBasedConstraint<NumMatches, double, long> constraint =
                new AnalysisBasedConstraint<NumMatches, double, long>(size, assertion, Option<Func<double, long>>.None,
                    hint);
            return new NamedConstraint<NumMatches, double, long>(constraint, $"SizeConstraint{size}");
        }

        public static IConstraint<FrequenciesAndNumRows, Distribution, Distribution> HistogramConstraint(
            string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            Option<string> hint,
            int maxBins = 1000
        )
        {
            var histogram = Histogram(column, binningFunc, where, maxBins);

            AnalysisBasedConstraint<FrequenciesAndNumRows, Distribution, Distribution> constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, Distribution, Distribution>(histogram, assertion,
                    Option<Func<Distribution, Distribution>>.None, hint);


            return new NamedConstraint<FrequenciesAndNumRows, Distribution, Distribution>(constraint,
                $"HistogramConstraint{histogram}");
        }

        public static IConstraint<FrequenciesAndNumRows, Distribution, long> HistogramBinConstraint(
            string column,
            Func<long, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            Option<string> hint,
            int maxBins = 1000
        )
        {
            var histogram = Histogram(column, binningFunc, where, maxBins);

            AnalysisBasedConstraint<FrequenciesAndNumRows, Distribution, long> constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, Distribution, long>(histogram, assertion,
                    new Func<Distribution, long>(_ => _.NumberOfBins),
                    hint);


            return new NamedConstraint<FrequenciesAndNumRows, Distribution, long>(constraint,
                $"HistogramBinConstraint{histogram}");
        }

        public static IConstraint<NumMatchesAndCount, double, double> CompletenessConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var completeness = Completeness(column, where);

            AnalysisBasedConstraint<NumMatchesAndCount, double, double> constraint =
                new AnalysisBasedConstraint<NumMatchesAndCount, double, double>(completeness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<NumMatchesAndCount, double, double>(constraint,
                $"CompletenessConstraint{completeness}");
        }

        public static IConstraint<S, Double, Double> AnomalyConstraint<S>(
            IAnalyzer<S, Metric<Double>> analyzer,
            Func<double, bool> anomalyAssertion,
            Option<string> hint
        ) where S : State<S>
        {
            var constraint = new AnalysisBasedConstraint<S, Double, Double>(analyzer, anomalyAssertion, hint);
            return new NamedConstraint<S, Double, Double>(constraint, $"AnomalyConstraint{analyzer}");
        }

        public static IConstraint<FrequenciesAndNumRows, double, double> UniquenessConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var uniqueness = Uniqueness(column, where);

            AnalysisBasedConstraint<FrequenciesAndNumRows, double, double> constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(uniqueness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<FrequenciesAndNumRows, double, double>(constraint,
                $"HistogramConstraint{uniqueness}");
        }

        public static IConstraint<FrequenciesAndNumRows, double, double> DistinctnessConstraint(
            IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var distinctness = Distinctness(columns, where);

            AnalysisBasedConstraint<FrequenciesAndNumRows, double, double> constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(distinctness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<FrequenciesAndNumRows, double, double>(constraint,
                $"DistinctnessConstraint{distinctness}");
        }


        public static IConstraint<NumMatchesAndCount, double, double> ComplianceConstraint(
            string name,
            Option<string> column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var compliance = Compliance(name, column.Value, where);

            AnalysisBasedConstraint<NumMatchesAndCount, double, double> constraint =
                new AnalysisBasedConstraint<NumMatchesAndCount, double, double>(compliance, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<NumMatchesAndCount, double, double>(constraint,
                $"ComplianceConstraint{constraint}");
        }

        public static IConstraint<FrequenciesAndNumRows, double, double> MutualInformationConstraint(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var mutualInformation = MutualInformation(new[] { columnA, columnB }.AsEnumerable(), where);

            AnalysisBasedConstraint<FrequenciesAndNumRows, double, double> constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(mutualInformation, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<FrequenciesAndNumRows, double, double>(constraint,
                $"MutualInformationConstraint{constraint}");
        }


        public static IConstraint<MaxState, double, double> MaxLengthConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var maxLength = MaxLength(column, where);

            AnalysisBasedConstraint<MaxState, double, double> constraint =
                new AnalysisBasedConstraint<MaxState, double, double>(maxLength, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<MaxState, double, double>(constraint,
                $"MaxLengthConstraint{constraint}");
        }

        public static IConstraint<MinState, double, double> MinLengthConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var minLength = MinLength(column, where);

            AnalysisBasedConstraint<MinState, double, double> constraint =
                new AnalysisBasedConstraint<MinState, double, double>(minLength, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<MinState, double, double>(constraint,
                $"MinLengthConstraint{constraint}");
        }

        public static IConstraint<MinState, double, double> MinConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var min = Minimum(column, where);

            AnalysisBasedConstraint<MinState, double, double> constraint =
                new AnalysisBasedConstraint<MinState, double, double>(min, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<MinState, double, double>(constraint,
                $"MinConstraint{constraint}");
        }

        public static IConstraint<MaxState, double, double> MaxConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var min = Maximum(column, where);

            AnalysisBasedConstraint<MaxState, double, double> constraint =
                new AnalysisBasedConstraint<MaxState, double, double>(min, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<MaxState, double, double>(constraint,
                $"MaxConstraint{constraint}");
        }

        public static IConstraint<MeanState, double, double> MeanConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var min = Mean(column, where);

            AnalysisBasedConstraint<MeanState, double, double> constraint =
                new AnalysisBasedConstraint<MeanState, double, double>(min, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<MeanState, double, double>(constraint,
                $"MeanConstraint{constraint}");
        }

        public static IConstraint<SumState, double, double> SumConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var sum = Sum(column, where);

            AnalysisBasedConstraint<SumState, double, double> constraint =
                new AnalysisBasedConstraint<SumState, double, double>(sum, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<SumState, double, double>(constraint,
                $"SumConstraint{constraint}");
        }

        public static IConstraint<StandardDeviationState, double, double> StandardDeviationConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var sum = StandardDeviation(column, where);

            AnalysisBasedConstraint<StandardDeviationState, double, double> constraint =
                new AnalysisBasedConstraint<StandardDeviationState, double, double>(sum, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint<StandardDeviationState, double, double>(constraint,
                $"StandardDeviationConstraint{constraint}");
        }

        public static IConstraint<StandardDeviationState, double, double> ApproxCountDistinctConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            throw new NotImplementedException();
        }

        public static IConstraint<StandardDeviationState, double, double> CorrelationConstraint(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            throw new NotImplementedException();
        }

        public static IConstraint<StandardDeviationState, double, double> DataTypeConstraint(
            string column,
            string columnB,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            throw new NotImplementedException();
        }
    }
}