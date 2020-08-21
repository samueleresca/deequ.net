using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using deequ.Analyzers;
using deequ.Analyzers.States;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using static deequ.Analyzers.Initializers;

namespace deequ.Constraints
{
    /// <summary>
    /// Interface that represent a constraint
    /// </summary>
    public interface IConstraint
    {
        /// <summary>
        /// Evaluates the constraint based on an analysis result
        /// </summary>
        /// <param name="analysisResult">The analysis result to evaluate against the check</param>
        /// <returns></returns>
        public ConstraintResult Evaluate(
            Dictionary<IAnalyzer<IMetric>, IMetric> analysisResult);
    }
    internal interface IAnalysisBasedConstraint : IConstraint
    {
        public IAnalyzer<IMetric> Analyzer { get; }
    }

    /// <summary>
    /// Represents the result of a constraint
    /// </summary>
    public class ConstraintResult
    {
        /// <summary>
        /// The metric used by the result
        /// </summary>
        public Option<IMetric> Metric;


        /// <summary>
        /// The constraint related to the constraint result <see cref="IConstraint"/>
        /// </summary>
        public IConstraint Constraint { get; set; }

        /// <summary>
        /// The status of the constraint <see cref="ConstraintStatus"/>
        /// </summary>
        public ConstraintStatus Status { get; }

        /// <summary>
        /// Optional message
        /// </summary>
        public Option<string> Message { get; }


        /// <summary>
        /// ctor of class <see cref="ConstraintResult"/>>
        /// </summary>
        /// <param name="constraint">The constraint binded with the result</param>
        /// <param name="status">The status of the constraint result</param>
        /// <param name="message">Optional message</param>
        /// <param name="metric"></param>
        public ConstraintResult(IConstraint constraint, ConstraintStatus status, Option<string> message,
            Option<IMetric> metric)
        {
            Constraint = constraint;
            Status = status;
            Message = message;
            Metric = metric;
        }

    }

    /// <summary>
    /// Status of the constraint
    /// </summary>
    public enum ConstraintStatus
    {
        /// <summary>
        /// Success
        /// </summary>
        Success = 0,
        /// <summary>
        /// Failure
        /// </summary>
        Failure = 1
    }


    internal class ConstraintDecorator : IConstraint
    {
        private readonly IConstraint _inner;

        public ConstraintDecorator(IConstraint constraint) => _inner = constraint;

        public IConstraint Inner
        {
            get
            {
                bool dc = _inner is ConstraintDecorator;
                if (!dc)
                {
                    return _inner;
                }

                ConstraintDecorator result = (ConstraintDecorator)_inner;
                return result.Inner;
            }
        }

        public ConstraintResult Evaluate(
            Dictionary<IAnalyzer<IMetric>, IMetric> analysisResult)
        {
            ConstraintResult result = _inner.Evaluate(analysisResult);
            result.Constraint = this;
            return result;
        }
    }

    internal class NamedConstraint : ConstraintDecorator
    {
        public NamedConstraint(IConstraint constraint, string name) : base(constraint) => _name = name;

        private string _name { get; }

        public override string ToString() => _name;
    }

    internal static class Functions
    {
        public static IConstraint SizeConstraint(Func<double, bool> assertion,
            Option<string> where, Option<string> hint)
        {
            Size size = Size(where);
            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(size, assertion,
                    Option<Func<double, double>>.None,
                    hint);
            return new NamedConstraint(constraint, $"SizeConstraint({size})");
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
            Histogram histogram = Histogram(column, binningFunc, where, maxBins);

            AnalysisBasedConstraint<Distribution, Distribution> constraint =
                new AnalysisBasedConstraint<Distribution, Distribution>(histogram, assertion,
                    Option<Func<Distribution, Distribution>>.None, hint);


            return new NamedConstraint(constraint,
                $"HistogramConstraint({histogram})");
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
            Histogram histogram = Histogram(column, binningFunc, where, maxBins);

            AnalysisBasedConstraint<Distribution, long> constraint =
                new AnalysisBasedConstraint<Distribution, long>(histogram, assertion,
                    new Func<Distribution, long>(target => target.NumberOfBins),
                    hint);


            return new NamedConstraint(constraint,
                $"HistogramBinConstraint({histogram})");
        }

        public static IConstraint CompletenessConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            IAnalyzer<IMetric> completeness = Completeness(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(completeness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"CompletenessConstraint({completeness})");
        }

        public static IConstraint AnomalyConstraint<S>(
            IAnalyzer<IMetric> analyzer,
            Func<double, bool> anomalyAssertion,
            Option<string> hint
        ) where S : IState
        {
            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(analyzer, anomalyAssertion, hint);
            return new NamedConstraint(constraint, $"AnomalyConstraint({analyzer})");
        }

        public static IConstraint UniquenessConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Uniqueness uniqueness = Uniqueness(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(uniqueness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"HistogramConstraint({uniqueness})");
        }

        public static IConstraint UniquenessConstraint(
            IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Uniqueness uniqueness = Uniqueness(columns, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(uniqueness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"HistogramConstraint({uniqueness})");
        }

        public static IConstraint DistinctnessConstraint(
            IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Distinctness distinctness = Distinctness(columns, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(distinctness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"DistinctnessConstraint({distinctness})");
        }


        public static IConstraint UniqueValueRatioConstraint(
            IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            IAnalyzer<IMetric> distinctness = UniqueValueRatio(columns, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(distinctness, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"UniqueValueRatioConstraint({distinctness})");
        }


        public static IConstraint ComplianceConstraint(
            string name,
            Option<Column> column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Compliance compliance = Compliance(name, column.Value, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(compliance, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"ComplianceConstraint({constraint})");
        }

        public static IConstraint MutualInformationConstraint(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            MutualInformation mutualInformation =
                MutualInformation(new[] { columnA, columnB }.AsEnumerable(), where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(mutualInformation, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MutualInformationConstraint({constraint})");
        }

        public static IConstraint EntropyConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Entropy entropy = Entropy(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(entropy, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"EntropyConstraint({constraint})");
        }

        public static IConstraint PatternMatchConstraint(
            string column,
            Regex pattern,
            Func<double, bool> assertion,
            Option<string> where = default,
            Option<string> name = default,
            Option<string> hint = default
        )
        {
            PatternMatch patternMatch = PatternMatch(column, pattern, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(patternMatch, assertion, hint);

            string constraintName = name.HasValue ? name.Value : $"PatternMatchConstraint({constraint})";
            return new NamedConstraint(constraint, constraintName);
        }

        public static IConstraint MaxLengthConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            MaxLength maxLength = MaxLength(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(maxLength, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MaxLengthConstraint({constraint})");
        }

        public static IConstraint MinLengthConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            MinLength minLength = MinLength(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(minLength, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MinLengthConstraint({constraint})");
        }

        public static IConstraint MinConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Minimum min = Minimum(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(min, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MinConstraint({constraint})");
        }

        public static IConstraint MaxConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Maximum max = Maximum(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(max, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MaxConstraint({constraint})");
        }

        public static IConstraint MeanConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Mean mean = Mean(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(mean, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"MeanConstraint({constraint})");
        }

        public static IConstraint SumConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Sum sum = Sum(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(sum, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"SumConstraint({constraint})");
        }

        public static IConstraint StandardDeviationConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            StandardDeviation stDev = StandardDeviation(column, where);

            AnalysisBasedConstraint<double, double> constraint =
                new AnalysisBasedConstraint<double, double>(stDev, assertion,
                    Option<Func<double, double>>.None, hint);

            return new NamedConstraint(constraint,
                $"StandardDeviationConstraint({constraint})");
        }

        public static IConstraint ApproxCountDistinctConstraint(
            string column,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        ) =>
            throw new NotImplementedException();

        public static IConstraint CorrelationConstraint(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        ) =>
            throw new NotImplementedException();

        public static IConstraint DataTypeConstraint(
            string column,
            ConstrainableDataTypes dataType,
            Func<double, bool> assertion,
            Option<string> where,
            Option<string> hint
        )
        {
            Func<Distribution, double> valuePicker = dataType == ConstrainableDataTypes.Numeric
                ? d => RatioTypes(true, DataTypeInstances.Fractional, d) +
                       RatioTypes(true, DataTypeInstances.Integral, d)
                : new Func<Distribution, double>(distribution =>
                {
                    Func<DataTypeInstances, double> pure =
                        keyType => RatioTypes(true, keyType, distribution);
                    return dataType switch
                    {
                        ConstrainableDataTypes.Null => RatioTypes(false, DataTypeInstances.Unknown, distribution),
                        ConstrainableDataTypes.Fractional => pure(DataTypeInstances.Fractional),
                        ConstrainableDataTypes.Integral => pure(DataTypeInstances.Integral),
                        ConstrainableDataTypes.Boolean => pure(DataTypeInstances.Boolean),
                        ConstrainableDataTypes.String => pure(DataTypeInstances.String)
                    };
                });

            DataType dataTypeResult = DataType(column, where);

            return new AnalysisBasedConstraint<Distribution, double>(dataTypeResult, assertion,
                valuePicker, hint);
        }


        private static double RatioTypes(bool ignoreUnknown, DataTypeInstances keyType, Distribution distribution)
        {
            if (!ignoreUnknown)
            {
                return distribution
                    .Values[keyType.ToString()]?
                    .Ratio ?? 0.0;
            }


            long absoluteCount = distribution
                .Values[keyType.ToString()]?
                .Absolute ?? 0L;

            if (absoluteCount == 0L)
            {
                return 0;
            }

            long numValues = distribution.Values.Sum(keyValuePair => keyValuePair.Value.Absolute);
            long numUnknown = distribution
                .Values[DataTypeInstances.Unknown.ToString()]?
                .Absolute ?? 0L;

            long sumOfNonNull = numValues - numUnknown;
            return (double)absoluteCount / sumOfNonNull;
        }
    }
}
