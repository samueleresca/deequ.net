using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Analyzers.States;
using xdeequ.AnomalyDetection;
using xdeequ.Constraints;
using xdeequ.Metrics;
using xdeequ.Repository;
using xdeequ.Util;
using static xdeequ.Constraints.Functions;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Checks
{
    public class CheckResult
    {

        public Check Check { get; set; }
        public CheckStatus Status { get; set; }
        public IEnumerable<ConstraintResult> ConstraintResults { get; set; }

        public CheckResult(Check check, CheckStatus status, IEnumerable<ConstraintResult> constraintResult)
        {
            Check = check;
            Status = status;
            ConstraintResults = constraintResult;
        }
    }

    public enum CheckLevel
    {
        Error = 0,
        Warning = 1
    }

    public enum CheckStatus
    {
        Success = 0,
        Warning = 1,
        Error = 2
    }

    public class Check
    {
        public static readonly Func<double, bool> IsOne = _ => _ == 1.0;

        public Check(CheckLevel level, string description, IEnumerable<IConstraint> constraints)
        {
            Level = level;
            Description = description;
            Constraints = constraints;
        }

        public Check(CheckLevel level, string description)
        {
            Level = level;
            Description = description;
            Constraints = new List<IConstraint>();
        }

        public CheckLevel Level { get; set; }
        public string Description { get; set; }
        public IEnumerable<IConstraint> Constraints { get; set; }

        private CheckWithLastConstraintFilterable AddFilterableConstraint(
            Func<Option<string>, IConstraint> constraintDefinition)
        {
            IConstraint constraintWithoutFiltering = constraintDefinition(Option<string>.None);
            IEnumerable<IConstraint> newConstraints = Constraints.Append(constraintWithoutFiltering);

            return new CheckWithLastConstraintFilterable(Level, Description, newConstraints, constraintDefinition);
        }

        public Check AddConstraint(IConstraint constraint)
        {
            Constraints = Constraints.Append(constraint);
            return this;
        }

        public CheckWithLastConstraintFilterable HasSize(Func<double, bool> assertion, Option<string> hint) =>
            AddFilterableConstraint(filter => SizeConstraint(assertion, filter, hint));

        public CheckWithLastConstraintFilterable IsComplete(string column, Option<string> hint) =>
            AddFilterableConstraint(filter => CompletenessConstraint(column, IsOne, filter, hint));

        public CheckWithLastConstraintFilterable HasCompleteness(string column, Func<double, bool> assertion,
            Option<string> hint) =>
            AddFilterableConstraint(filter => CompletenessConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable AreComplete(IEnumerable<string> columns, Option<string> hint) =>
            Satisfies(ChecksExt.IsEachNotNull(columns), "Combined Completeness", IsOne, hint);

        public CheckWithLastConstraintFilterable HaveCompleteness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint) =>
            Satisfies(ChecksExt.IsEachNotNull(columns), "Combined Completeness", assertion, hint);

        public CheckWithLastConstraintFilterable AreAnyComplete(IEnumerable<string> columns, Option<string> hint) =>
            Satisfies(ChecksExt.IsAnyNotNull(columns), "Any Completeness", IsOne, hint);

        public CheckWithLastConstraintFilterable HaveAnyCompleteness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint) =>
            Satisfies(ChecksExt.IsAnyNotNull(columns), "Any Completeness", assertion, hint);

        public CheckWithLastConstraintFilterable IsUnique(string column, Option<string> hint) =>
            AddFilterableConstraint(filter => UniquenessConstraint(column, IsOne, filter, hint));

        public CheckWithLastConstraintFilterable IsPrimaryKey(string column, IEnumerable<string> columns) =>
            AddFilterableConstraint(filter =>
                UniquenessConstraint(new[] { column }.Concat(columns), IsOne, filter, Option<string>.None));

        public CheckWithLastConstraintFilterable IsPrimaryKey(string column, Option<string> hint,
            IEnumerable<string> columns) =>
            AddFilterableConstraint(filter =>
                UniquenessConstraint(new[] { column }.Concat(columns), IsOne, filter, hint));

        public CheckWithLastConstraintFilterable HasUniqueness(IEnumerable<string> columns,
            Func<double, bool> assertion) =>
            AddFilterableConstraint(filter =>
                UniquenessConstraint(columns, assertion, filter, Option<string>.None));

        public CheckWithLastConstraintFilterable HasUniqueness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint) =>
            AddFilterableConstraint(filter => UniquenessConstraint(columns, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasUniqueness(string column, Func<double, bool> assertion) =>
            AddFilterableConstraint(filter =>
                UniquenessConstraint(column, assertion, filter, Option<string>.None));

        public CheckWithLastConstraintFilterable HasUniqueness(string column, Func<double, bool> assertion,
            Option<string> hint) =>
            AddFilterableConstraint(filter => UniquenessConstraint(column, assertion, filter, hint));


        public CheckWithLastConstraintFilterable HasDistinctness(IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> hint) =>
            AddFilterableConstraint(filter => DistinctnessConstraint(columns, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasUniqueValueRatio(IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> hint) =>
            AddFilterableConstraint(filter => UniqueValueRatioConstraint(columns, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasNumberOfDistinctValues(string column,
            Func<long, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> hint,
            int maxBins = 1000
        ) =>
            AddFilterableConstraint(filter =>
                HistogramBinConstraint(column, assertion, binningFunc, filter, hint, maxBins));


        public CheckWithLastConstraintFilterable HasHistogramValues(string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> hint,
            int maxBins = 100
        ) =>
            AddFilterableConstraint(filter =>
                HistogramConstraint(column, assertion, binningFunc, filter, hint, maxBins));

        public CheckWithLastConstraintFilterable KllSketchSatisfies(string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> hint,
            int maxBins = 100
        ) =>
            throw new NotImplementedException();

        public Check IsNewestPointNonAnomalous<S>(
            IMetricsRepository metricRepository,
            IAnomalyDetectionStrategy anomalyDetectionStrategy,
            IAnalyzer<IMetric> analyzer,
            Dictionary<string, string> withTagValues,
            Option<long> afterDate,
            Option<long> beforeDate
        ) where S : IState
        {
            return AddConstraint(AnomalyConstraint<S>(analyzer, d =>
                IsNewestPointNonAnomalous(metricRepository, anomalyDetectionStrategy, withTagValues, afterDate,
                beforeDate, analyzer, d), Option<string>.None));
        }

        public CheckWithLastConstraintFilterable HasEntropy(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => EntropyConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasMutualInformation(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter =>
                MutualInformationConstraint(columnA, columnB, assertion, filter, hint));


        public CheckWithLastConstraintFilterable HasApproxQuantile(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            throw new NotImplementedException();

        public CheckWithLastConstraintFilterable HasMinLength(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => MinLengthConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasMaxLength(string column,
            Func<double, bool> assertion,
            Option<string> hint,
            int maxBins = 100
        ) =>
            AddFilterableConstraint(filter => MaxLengthConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasMin(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => MinConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasMax(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => MaxConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasMean(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => MeanConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasSum(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => SumConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasStandardDeviation(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => StandardDeviationConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasApproxCountDistinct(string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => ApproxCountDistinctConstraint(column, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasCorrelation(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => CorrelationConstraint(columnA, columnB, assertion, filter, hint));

        public CheckWithLastConstraintFilterable Satisfies(string columnCondition, string constraintName,
            Func<double, bool> assertion, Option<string> hint) =>
            Satisfies(Expr(columnCondition), constraintName, assertion, hint);

        public CheckWithLastConstraintFilterable Satisfies(string columnCondition, string constraintName,
            Option<string> hint) => Satisfies(Expr(columnCondition), constraintName, hint);

        public CheckWithLastConstraintFilterable Satisfies(Column columnCondition, string constraintName,
            Func<double, bool> assertion, Option<string> hint) =>
            AddFilterableConstraint(filter =>
                ComplianceConstraint(constraintName, columnCondition, assertion, filter, hint));

        public CheckWithLastConstraintFilterable Satisfies(Column columnCondition, string constraintName,
            Option<string> hint) =>
            AddFilterableConstraint(filter =>
                ComplianceConstraint(constraintName, columnCondition, IsOne, filter, hint));

        public CheckWithLastConstraintFilterable HasPattern(
            string column,
            Regex pattern,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter =>
                PatternMatchConstraint(column, pattern, assertion, filter, hint));

        public CheckWithLastConstraintFilterable HasPattern(
            string column,
            Regex pattern,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter =>
                PatternMatchConstraint(column, pattern, IsOne, filter, hint));

        public CheckWithLastConstraintFilterable ContainsCreditCardNumber(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            HasPattern(column, Patterns.CreditCard, assertion, $"ContainsCreditCardNumber({column})");

        public CheckWithLastConstraintFilterable ContainsEmail(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            HasPattern(column, Patterns.Email, assertion, $"ContainsEmail({column})");

        public CheckWithLastConstraintFilterable ContainsURL(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            HasPattern(column, Patterns.Url, assertion, $"ContainsURL({column})");

        public CheckWithLastConstraintFilterable ContainsSSN(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            HasPattern(column, Patterns.SocialSecurityNumberUs, assertion, $"ContainsSSN({column})");

        public CheckWithLastConstraintFilterable HasDataType(
            string column,
            ConstrainableDataTypes dataType,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            AddFilterableConstraint(filter => DataTypeConstraint(column, dataType, assertion, filter, hint));

        public CheckWithLastConstraintFilterable IsNonNegative(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            Satisfies(Expr($"COALESCE({column}, 0.0) >= 0"), $"{column} is non-negative", assertion, hint);

        public CheckWithLastConstraintFilterable IsNonNegative(
            string column,
            Option<string> hint
        ) =>
            Satisfies(Expr($"COALESCE({column}, 0.0) >= 0"), $"{column} is non-negative", hint);

        public CheckWithLastConstraintFilterable IsPositive(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            Satisfies(Expr($"COALESCE({column}, 1.0) >= 0"), $"{column} is positive", assertion, hint);

        public CheckWithLastConstraintFilterable IsPositive(
            string column,
            Option<string> hint
        ) =>
            Satisfies(Expr($"COALESCE({column}, 1.0) >= 0"), $"{column} is positive", hint);

        public CheckWithLastConstraintFilterable IsLessThan(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} < {columnB}"), $"{columnA} is less than {columnB}", assertion, hint);


        public CheckWithLastConstraintFilterable IsLessThan(
            string columnA,
            string columnB,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} < {columnB}"), $"{columnA} is less than {columnB}", hint);

        public CheckWithLastConstraintFilterable IsLessThanOrEqualTo(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} <= {columnB}"), $"{columnA} is less than or equal to {columnB}",
                assertion,
                hint);


        public CheckWithLastConstraintFilterable IsLessThanOrEqualTo(
            string columnA,
            string columnB,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} <= {columnB}"), $"{columnA} is less than or equal to {columnB}",
                hint);

        public CheckWithLastConstraintFilterable IsGreaterThan(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} > {columnB}"), $"{columnA} is greater than {columnB}", assertion, hint);

        public CheckWithLastConstraintFilterable IsGreaterThan(
            string columnA,
            string columnB,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} > {columnB}"), $"{columnA} is greater than {columnB}", hint);

        public CheckWithLastConstraintFilterable IsGreaterOrEqualTo(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} >= {columnB}"), $"{columnA} is greater than or equal to {columnB}",
                assertion,
                hint);

        public CheckWithLastConstraintFilterable IsGreaterOrEqualTo(
            string columnA,
            string columnB,
            Option<string> hint
        ) =>
            Satisfies(Expr($"{columnA} >= {columnB}"), $"{columnA} is greater than or equal to {columnB}",
                hint);

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues
        ) =>
            IsContainedIn(column, allowedValues, IsOne, Option<string>.None);

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Option<string> hint
        ) =>
            IsContainedIn(column, allowedValues, IsOne, hint);

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Func<double, bool> assertion
        ) =>
            IsContainedIn(column, allowedValues, assertion, Option<string>.None);

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            double lowerBound,
            double upperBound,
            Option<string> hint,
            bool includeUpperBound = true,
            bool includeLowerBound = true
        )
        {
            string leftOperand = includeLowerBound ? ">=" : ">";
            string rightOperand = includeUpperBound ? "<=" : "<";

            string predictate = $"{column} IS NULL OR" +
                                $"(`{column}` {leftOperand} {lowerBound} AND {column} {rightOperand} {upperBound} )";

            return Satisfies(Expr(predictate), $"{column} between {lowerBound} and {upperBound}", hint);
        }

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            string valueList = "'" + string.Join("', '", allowedValues) + "'";

            string predictate = $"{column} IS NULL OR" +
                                $"(`{column}` IN ({valueList}) )";

            if (assertion == null)
            {
                return Satisfies(Expr(predictate),
                    $"{column} contained in {string.Join(",", allowedValues)}", hint);
            }

            return Satisfies(Expr(predictate),
                $"{column} contained in {string.Join(",", allowedValues)}", assertion, hint);
        }


        public CheckResult Evaluate(AnalyzerContext context)
        {
            IEnumerable<ConstraintResult> constraintResults = Constraints.Select(x => x.Evaluate(context.MetricMap));
            bool anyFailure = constraintResults.Any(x => x.Status == ConstraintStatus.Failure);

            CheckStatus checkStatus = (anyFailure, Level) switch
            {
                (true, CheckLevel.Error) => CheckStatus.Error,
                (true, CheckLevel.Warning) => CheckStatus.Warning,
                _ => CheckStatus.Success
            };

            return new CheckResult(this, checkStatus, constraintResults);
        }


        public IEnumerable<IAnalyzer<IMetric>> RequiredAnalyzers() =>
            Constraints
                .Select(cons =>
                {
                    if (!(cons is ConstraintDecorator))
                    {
                        return cons;
                    }

                    ConstraintDecorator nc = (ConstraintDecorator)cons;
                    return nc.Inner;
                })
                .OfType<IAnalysisBasedConstraint>()
                .Select(x => x.Analyzer);

        private bool IsNewestPointNonAnomalous(IMetricsRepository metricsRepository,
            IAnomalyDetectionStrategy anomalyDetectionStrategy,
            Dictionary<string, string> valueWithTagValues,
            Option<long> valueAfterDate,
            Option<long> valueBeforeDate,
            IAnalyzer<IMetric> analyzer,
            double currentMetricValue)
        {

            // Get history keys
            var repositoryLoader = metricsRepository.Load();
            repositoryLoader = repositoryLoader.WithTagValues(valueWithTagValues);

            valueBeforeDate.OnSuccess((value) =>
            {
                repositoryLoader = repositoryLoader.Before(value);
            });

            valueAfterDate.OnSuccess((value) =>
            {
                repositoryLoader = repositoryLoader.After(value);
            });

            repositoryLoader = repositoryLoader.ForAnalyzers(new[] { analyzer });

            var analysisResults = repositoryLoader.Get();

            if (!analysisResults.Any())
            {
                throw new ArgumentException("There have to be previous results in the MetricsRepository!");
            }


            var historicalMetrics = analysisResults
                .OrderBy(x => x.ResultKey.Tags.Values)
                .Select(analysisResults =>
                {
                    var analyzerContextMetricMap = analysisResults.AnalyzerContext.MetricMap;
                    var onlyAnalyzerMetricEntryInLoadedAnalyzerContext = analyzerContextMetricMap.FirstOrDefault();
                    var doubleMetricOption = (Metric<double>)onlyAnalyzerMetricEntryInLoadedAnalyzerContext.Value;

                    var dataSetDate = analysisResults.ResultKey.DataSetDate;

                    return (dataSetDate, doubleMetricOption);
                });

            var testDateTime = analysisResults.Select(x => x.ResultKey.DataSetDate).Max() + 1;

            if (testDateTime == long.MaxValue)
            {
                throw new ArgumentException("Test DateTime cannot be Long.MaxValue, otherwise the" +
                                            "Anomaly Detection, which works with an open upper interval bound, won't test anything");
            }

            var anomalyDetector = new AnomalyDetector(anomalyDetectionStrategy);
            var metricsOptions = historicalMetrics
                .Select(pair =>
                {
                    Option<double> valueOption = Option<double>.None;

                    if (pair.doubleMetricOption.IsSuccess())
                    {
                        valueOption = new Option<double>(pair.doubleMetricOption.Value.Get());
                    }

                    return new DataPoint<double>(pair.dataSetDate, valueOption);
                });
            var detectedAnomalies = anomalyDetector.IsNewPointAnomalous(metricsOptions,
                new DataPoint<double>(testDateTime, new Option<double>(currentMetricValue)));

            return !detectedAnomalies.Anomalies.Any();
        }
    }
}
