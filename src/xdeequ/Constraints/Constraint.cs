using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;
using static xdeequ.Analyzers.Initializers;

namespace xdeequ.Constraints
{
    public interface IConstraint
    {
        public ConstraintResult Evaluate(
            Dictionary<IAnalyzer<IMetric>, IMetric> analysisResult);
    }

    public interface IAnalysisBasedConstraint : IConstraint
    {
        public IAnalyzer<IMetric> Analyzer { get; }
    }

    public class ConstraintResult
    {
        public Option<IMetric> Metric;


        public ConstraintResult(IConstraint constraint, ConstraintStatus status, Option<string> message,
            Option<IMetric> metric)
        {
            Constraint = constraint;
            Status = status;
            Message = message;
            Metric = metric;
        }

        public IConstraint Constraint { get; set; }
        public ConstraintStatus Status { get; set; }
        public Option<string> Message { get; set; }
    }

    public enum ConstraintStatus
    {
        Success = 0,
        Failure = 1
    }


    public class ConstraintDecorator : IConstraint
    {
        private readonly IConstraint _inner;

        public ConstraintDecorator(IConstraint constraint)
        {
            _inner = constraint;
        }

        public IConstraint Inner
        {
            get
            {
                var dc = _inner is ConstraintDecorator;
                if (!dc) return _inner;
                var result = (ConstraintDecorator)_inner;
                return result.Inner;
            }
        }

        public ConstraintResult Evaluate(
            Dictionary<IAnalyzer<IMetric>, IMetric> analysisResult)
        {
            var result = _inner.Evaluate(analysisResult);
            result.Constraint = this;
            return result;
        }
    }

    /**
  * Constraint decorator which holds a name of the constraint along with it
  *
  * @param constraint Delegate
  * @param name       Name (Detailed message) for the constraint
  */
    public class NamedConstraint : ConstraintDecorator
    {
        public NamedConstraint(IConstraint constraint, string name) : base(constraint)
        {
            _name = name;
        }

        private string _name { get; }

        public override string ToString()
        {
            return _name;
        }
    }

    public static class Functions
    {
        public static IConstraint SizeConstraint(Func<double, bool> assertion,
            Option<string> where, Option<string> hint)
        {
            Size size = Size(where);
            var constraint =
                new AnalysisBasedConstraint<NumMatches, double, double>(size, assertion, Option<Func<double, double>>.None,
                    hint);
            return new NamedConstraint(constraint, $"SizeConstraint{size}");
        }

        public static IConstraint HistogramConstraint(
            string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            Option<string> hint,
            int maxBins = 1000
        )
        {
            var histogram = Histogram(column, binningFunc, @where, maxBins);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, Distribution, Distribution>(histogram, assertion,
                    Option<Func<Distribution, Distribution>>.None, hint);


            return new NamedConstraint(constraint,
                $"HistogramConstraint{histogram}");
        }

        public static IConstraint HistogramBinConstraint(
            string column,
            Func<long, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> where,
            Option<string> hint,
            int maxBins = 1000
        )
        {
            var histogram = Histogram(column, binningFunc, @where, maxBins);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, Distribution, long>(histogram, assertion,
                    new Func<Distribution, long>(_ => _.NumberOfBins),
                    hint);


            return new NamedConstraint(constraint,
                $"HistogramBinConstraint{histogram}");
        }

        public static IConstraint CompletenessConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            IAnalyzer<IMetric> completeness = Completeness(column, where);

            var constraint =
                new AnalysisBasedConstraint<NumMatchesAndCount, double, double>(completeness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"CompletenessConstraint{completeness}");
        }

        public static IConstraint AnomalyConstraint<S>(
            IAnalyzer<IMetric> analyzer,
            Func<double, bool> anomalyAssertion,
            Option<string> hint
        ) where S : State<S>, IState
        {
            var constraint = new AnalysisBasedConstraint<S, double, double>(analyzer, anomalyAssertion, hint);
            return new NamedConstraint(constraint, $"AnomalyConstraint{analyzer}");
        }

        public static IConstraint UniquenessConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var uniqueness = Uniqueness(column, where);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(uniqueness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"HistogramConstraint{uniqueness}");
        }

        public static IConstraint UniquenessConstraint(
            IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var uniqueness = Uniqueness(columns, where);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(uniqueness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"HistogramConstraint{uniqueness}");
        }

        public static IConstraint DistinctnessConstraint(
            IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var distinctness = Distinctness(columns, where);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(distinctness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"DistinctnessConstraint{distinctness}");
        }


        public static IConstraint UniqueValueRatioConstraint(
            IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            IAnalyzer<IMetric> distinctness = UniqueValueRatio(columns, where);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(distinctness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"UniqueValueRatioConstraint{distinctness}");
        }


        public static IConstraint ComplianceConstraint(
            string name,
            Option<Column> column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var compliance = Compliance(name, column.Value, where);

            var constraint =
                new AnalysisBasedConstraint<NumMatchesAndCount, double, double>(compliance, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"ComplianceConstraint{constraint}");
        }

        public static IConstraint MutualInformationConstraint(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var mutualInformation =
                MutualInformation(new[] { columnA, columnB }.AsEnumerable(), where);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(mutualInformation, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MutualInformationConstraint{constraint}");
        }

        public static IConstraint EntropyConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var entropy = Entropy(column, where);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(entropy, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"EntropyConstraint{constraint}");
        }

        public static IConstraint PatternMatchConstraint(
            string column,
            Regex pattern,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> name,
            Option<string> hint
        )
        {
            var patternMatch = PatternMatch(column, pattern, where);

            var constraint =
                new AnalysisBasedConstraint<FrequenciesAndNumRows, double, double>(patternMatch, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"PatternMatchConstraint{constraint}");
        }

        public static IConstraint MaxLengthConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var maxLength = MaxLength(column, where);

            var constraint =
                new AnalysisBasedConstraint<MaxState, double, double>(maxLength, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MaxLengthConstraint{constraint}");
        }

        public static IConstraint MinLengthConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var minLength = MinLength(column, where);

            var constraint =
                new AnalysisBasedConstraint<MinState, double, double>(minLength, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MinLengthConstraint{constraint}");
        }

        public static IConstraint MinConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var min = Minimum(column, where);

            var constraint =
                new AnalysisBasedConstraint<MinState, double, double>(min, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MinConstraint{constraint}");
        }

        public static IConstraint MaxConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var min = Maximum(column, where);

            var constraint =
                new AnalysisBasedConstraint<MaxState, double, double>(min, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MaxConstraint{constraint}");
        }

        public static IConstraint MeanConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var min = Mean(column, where);

            var constraint =
                new AnalysisBasedConstraint<MeanState, double, double>(min, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MeanConstraint{constraint}");
        }

        public static IConstraint SumConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var sum = Sum(column, where);

            var constraint =
                new AnalysisBasedConstraint<SumState, double, double>(sum, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"SumConstraint{constraint}");
        }

        public static IConstraint StandardDeviationConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var sum = StandardDeviation(column, where);

            var constraint =
                new AnalysisBasedConstraint<StandardDeviationState, double, double>(sum, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"StandardDeviationConstraint{constraint}");
        }

        public static IConstraint ApproxCountDistinctConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            throw new NotImplementedException();
        }

        public static IConstraint CorrelationConstraint(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            throw new NotImplementedException();
        }

        public static IConstraint DataTypeConstraint(
            string column,
            ConstrainableDataTypes dataType,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            var valuePicker = dataType == ConstrainableDataTypes.Numeric
                ? d => RatioTypes(true, DataTypeInstances.Fractional, d) +
                       RatioTypes(true, DataTypeInstances.Integral, d)
                : new Func<Distribution, double>(distribution =>
                {
                    var pure = new Func<DataTypeInstances, double>(keyType => RatioTypes(true, keyType, distribution));
                    return dataType switch
                    {
                        ConstrainableDataTypes.Null => RatioTypes(false, DataTypeInstances.Unknown, distribution),
                        ConstrainableDataTypes.Fractional => pure(DataTypeInstances.Fractional),
                        ConstrainableDataTypes.Integral => pure(DataTypeInstances.Integral),
                        ConstrainableDataTypes.Boolean => pure(DataTypeInstances.Boolean),
                        ConstrainableDataTypes.String => pure(DataTypeInstances.String)
                    };
                });

            var dataTypeResult = DataType(column, where);

            return new AnalysisBasedConstraint<DataTypeHistogram, Distribution, double>(dataTypeResult, assertion,
                valuePicker, hint);
        }


        private static double RatioTypes(bool ignoreUnknown, DataTypeInstances keyType, Distribution distribution)
        {
            if (!ignoreUnknown)
                return distribution
                    .Values[keyType.ToString()]?
                    .Ratio ?? 0.0;


            var absoluteCount = distribution
                .Values[keyType.ToString()]?
                .Absolute ?? 0L;

            if (absoluteCount == 0L)
                return 0;

            var numValues = distribution.Values.Sum(x => x.Value.Absolute);
            var numUnknown = distribution
                .Values[DataTypeInstances.Unknown.ToString()]?
                .Absolute ?? 0L;

            var sumOfNonNull = numValues - numUnknown;
            return (double)absoluteCount / sumOfNonNull;
        }
    }
}