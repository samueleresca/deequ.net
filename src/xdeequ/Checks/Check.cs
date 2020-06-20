using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Analyzers.States;
using xdeequ.Constraints;
using xdeequ.Metrics;
using xdeequ.Util;
using static xdeequ.Constraints.Functions;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Checks
{
    public class CheckResult
    {
        public CheckResult(Check check, CheckStatus status, IEnumerable<ConstraintResult> constraintResult)
        {
            Check = check;
            Status = status;
            ConstraintResults = constraintResult;
        }

        public Check Check { get; set; }
        public CheckStatus Status { get; set; }
        public IEnumerable<ConstraintResult> ConstraintResults { get; set; }
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
        protected IEnumerable<IConstraint> Constraints { get; set; }

        private CheckWithLastConstraintFilterable AddFilterableConstraint(
            Func<Option<string>, IConstraint> constraintDefinition)
        {
            var constraintWithoutFiltering = constraintDefinition(Option<string>.None);
            var newConstraints = Constraints.Append(constraintWithoutFiltering);

            return new CheckWithLastConstraintFilterable(Level, Description, newConstraints, constraintDefinition);
        }

        public Check AddConstraint(IConstraint constraint)
        {
            Constraints = Constraints.Append(constraint);
            return this;
        }

        public CheckWithLastConstraintFilterable HasSize(Func<double, bool> assertion, Option<string> hint)
        {
            return AddFilterableConstraint(filter => SizeConstraint(assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable IsComplete(string column, Option<string> hint)
        {
            return AddFilterableConstraint(filter => CompletenessConstraint(column, IsOne, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasCompleteness(string column, Func<double, bool> assertion,
            Option<string> hint)
        {
            return AddFilterableConstraint(filter => CompletenessConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable AreComplete(IEnumerable<string> columns, Option<string> hint)
        {
            return Satisfies(ChecksExt.IsEachNotNull(columns), "Combined Completeness", IsOne, hint);
        }

        public CheckWithLastConstraintFilterable HaveCompleteness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint)
        {
            return Satisfies(ChecksExt.IsEachNotNull(columns), "Combined Completeness", assertion, hint);
        }

        public CheckWithLastConstraintFilterable AreAnyComplete(IEnumerable<string> columns, Option<string> hint)
        {
            return Satisfies(ChecksExt.IsAnyNotNull(columns), "Any Completeness", IsOne, hint);
        }

        public CheckWithLastConstraintFilterable HaveAnyCompleteness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint)
        {
            return Satisfies(ChecksExt.IsAnyNotNull(columns), "Any Completeness", assertion, hint);
        }

        public CheckWithLastConstraintFilterable IsUnique(string column, Option<string> hint)
        {
            return AddFilterableConstraint(filter => UniquenessConstraint(column, IsOne, filter, hint));
        }

        public CheckWithLastConstraintFilterable IsPrimaryKey(string column, IEnumerable<string> columns)
        {
            return AddFilterableConstraint(filter =>
                UniquenessConstraint(new[] { column }.Concat(columns), IsOne, filter, Option<string>.None));
        }

        public CheckWithLastConstraintFilterable IsPrimaryKey(string column, Option<string> hint,
            IEnumerable<string> columns)
        {
            return AddFilterableConstraint(filter =>
                UniquenessConstraint(new[] { column }.Concat(columns), IsOne, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasUniqueness(IEnumerable<string> columns,
            Func<double, bool> assertion)
        {
            return AddFilterableConstraint(filter =>
                UniquenessConstraint(columns, assertion, filter, Option<string>.None));
        }

        public CheckWithLastConstraintFilterable HasUniqueness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint)
        {
            return AddFilterableConstraint(filter => UniquenessConstraint(columns, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasUniqueness(string column, Func<double, bool> assertion)
        {
            return AddFilterableConstraint(filter =>
                UniquenessConstraint(column, assertion, filter, Option<string>.None));
        }

        public CheckWithLastConstraintFilterable HasUniqueness(string column, Func<double, bool> assertion,
            Option<string> hint)
        {
            return AddFilterableConstraint(filter => UniquenessConstraint(column, assertion, filter, hint));
        }


        public CheckWithLastConstraintFilterable HasDistinctness(IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> hint)
        {
            return AddFilterableConstraint(filter => DistinctnessConstraint(columns, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasUniqueValueRatio(IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> hint)
        {
            return AddFilterableConstraint(filter => UniqueValueRatioConstraint(columns, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasNumberOfDistinctValues(string column,
            Func<long, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> hint,
            int maxBins = 1000
        )
        {
            return AddFilterableConstraint(filter =>
                HistogramBinConstraint(column, assertion, binningFunc, filter, hint, maxBins));
        }



        public CheckWithLastConstraintFilterable HasHistogramValues(string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> hint,
            int maxBins = 100
        )
        {
            return AddFilterableConstraint(filter =>
                HistogramConstraint(column, assertion, binningFunc, filter, hint, maxBins));
        }

        public CheckWithLastConstraintFilterable KllSketchSatisfies(string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> hint,
            int maxBins = 100
        )
        {
            throw new NotImplementedException();
        }

        public CheckWithLastConstraintFilterable IsNewestPointNonAnomalous(string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc,
            Option<string> hint,
            int maxBins = 100
        )
        {
            throw new NotImplementedException();
        }

        public CheckWithLastConstraintFilterable HasEntropy(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => EntropyConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasMutualInformation(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter =>
                MutualInformationConstraint(columnA, columnB, assertion, filter, hint));
        }


        public CheckWithLastConstraintFilterable HasApproxQuantile(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            throw new NotImplementedException();
        }

        public CheckWithLastConstraintFilterable HasMinLength(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => MinLengthConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasMaxLength(string column,
            Func<double, bool> assertion,
            Option<string> hint,
            int maxBins = 100
        )
        {
            return AddFilterableConstraint(filter => MaxLengthConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasMin(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => MinConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasMax(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => MaxConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasMean(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => MeanConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasSum(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => SumConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasStandardDeviation(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => StandardDeviationConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasApproxCountDistinct(string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => ApproxCountDistinctConstraint(column, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasCorrelation(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => CorrelationConstraint(columnA, columnB, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable Satisfies(string columnCondition, string constraintName,
            Func<double, bool> assertion, Option<string> hint)
        {
            return Satisfies(Expr(columnCondition), constraintName, assertion, hint);
        }

        public CheckWithLastConstraintFilterable Satisfies(string columnCondition, string constraintName, Option<string> hint)
        {
            return Satisfies(Expr(columnCondition), constraintName, hint);
        }

        public CheckWithLastConstraintFilterable Satisfies(Column columnCondition, string constraintName,
            Func<double, bool> assertion, Option<string> hint)
        {
            return AddFilterableConstraint(filter =>
                ComplianceConstraint(constraintName, columnCondition, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable Satisfies(Column columnCondition, string constraintName,
            Option<string> hint)
        {
            return AddFilterableConstraint(filter =>
                ComplianceConstraint(constraintName, columnCondition, IsOne, filter, hint));
        }

        public CheckWithLastConstraintFilterable HasPattern(
            string column,
            Regex pattern,
            Func<double, bool> assertion,
            Option<string> name,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter =>
                PatternMatchConstraint(column, pattern, assertion, filter, name, hint));
        }

        public CheckWithLastConstraintFilterable ContainsCreditCardNumber(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return HasPattern(column, new Regex(string.Empty), assertion, $"ContainsCreditCardNumber({column})", hint);
        }

        public CheckWithLastConstraintFilterable ContainsEmail(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return HasPattern(column, new Regex(string.Empty), assertion, $"ContainsEmail({column})", hint);
        }

        public CheckWithLastConstraintFilterable ContainsURL(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return HasPattern(column, new Regex(string.Empty), assertion, $"ContainsURL({column})", hint);
        }

        public CheckWithLastConstraintFilterable ContainsSSN(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return HasPattern(column, new Regex(string.Empty), assertion, $"ContainsSSN({column})", hint);
        }

        public CheckWithLastConstraintFilterable HasDataType(
            string column,
            ConstrainableDataTypes dataType,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return AddFilterableConstraint(filter => DataTypeConstraint(column, dataType, assertion, filter, hint));
        }

        public CheckWithLastConstraintFilterable IsNonNegative(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"COALESCE({column}, 0.0) >= 0"), $"{column} is non-negative", assertion, hint);
        }

        public CheckWithLastConstraintFilterable IsNonNegative(
            string column,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"COALESCE({column}, 0.0) >= 0"), $"{column} is non-negative", hint);
        }

        public CheckWithLastConstraintFilterable IsPositive(
            string column,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"COALESCE({column}, 1.0) >= 0"), $"{column} is positive", assertion, hint);
        }

        public CheckWithLastConstraintFilterable IsPositive(
            string column,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"COALESCE({column}, 1.0) >= 0"), $"{column} is positive", hint);
        }

        public CheckWithLastConstraintFilterable IsLessThan(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} < {columnB}"), $"{columnA} is less than {columnB}", assertion, hint);
        }


        public CheckWithLastConstraintFilterable IsLessThan(
            string columnA,
            string columnB,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} < {columnB}"), $"{columnA} is less than {columnB}", hint);
        }

        public CheckWithLastConstraintFilterable IsLessThanOrEqualTo(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} <= {columnB}"), $"{columnA} is less than or equal to {columnB}",
                assertion,
                hint);
        }


        public CheckWithLastConstraintFilterable IsLessThanOrEqualTo(
            string columnA,
            string columnB,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} <= {columnB}"), $"{columnA} is less than or equal to {columnB}",
                hint);
        }

        public CheckWithLastConstraintFilterable IsGreaterThan(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} > {columnB}"), $"{columnA} is greater than {columnB}", assertion, hint);
        }

        public CheckWithLastConstraintFilterable IsGreaterThan(
            string columnA,
            string columnB,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} > {columnB}"), $"{columnA} is greater than {columnB}", hint);
        }

        public CheckWithLastConstraintFilterable IsGreaterOrEqualTo(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} >= {columnB}"), $"{columnA} is greater than or equal to {columnB}",
                assertion,
                hint);
        }

        public CheckWithLastConstraintFilterable IsGreaterOrEqualTo(
            string columnA,
            string columnB,
            Option<string> hint
        )
        {
            return Satisfies(Expr($"{columnA} >= {columnB}"), $"{columnA} is greater than or equal to {columnB}",
                hint);
        }

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues
        )
        {
            return IsContainedIn(column, allowedValues, IsOne, Option<string>.None);
        }

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Option<string> hint
        )
        {
            return IsContainedIn(column, allowedValues, IsOne, hint);
        }

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Func<double, bool> assertion
        )
        {
            return IsContainedIn(column, allowedValues, assertion, Option<string>.None);
        }

        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            double lowerBound,
            double upperBound,
            Option<string> hint,
            bool includeUpperBound = true,
            bool includeLowerBound = true
        )
        {
            var leftOperand = includeLowerBound ? ">=" : ">";
            var rightOperand = includeUpperBound ? "<=" : "<";

            var predictate = $"{column} IS NULL OR" +
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

            var valueList = "'" + string.Join("', '", allowedValues) + "'";

            var predictate = $"{column} IS NULL OR" +
                             $"(`{column}` IN ({valueList}) )";

            if (assertion == null) return Satisfies(Expr(predictate),
                 $"{column} contained in {string.Join(",", allowedValues)}", hint);

            return Satisfies(Expr(predictate),
                $"{column} contained in {string.Join(",", allowedValues)}", assertion, hint);
        }


        public CheckResult Evaluate(AnalyzerContext context)
        {
            var constraintResults = Constraints.Select(x => x.Evaluate(context.MetricMap));
            var anyFailure = constraintResults.Any(x => x.Status == ConstraintStatus.Failure);

            var checkStatus = (anyFailure, Level) switch
            {
                (true, CheckLevel.Error) => CheckStatus.Error,
                (true, CheckLevel.Warning) => CheckStatus.Warning,
                _ => CheckStatus.Success
            };

            return new CheckResult(this, checkStatus, constraintResults);
        }


        public IEnumerable<IAnalyzer<IMetric>> RequiredAnalyzers()
        {
            return Constraints
                .Select(cons =>
                {
                    if (!(cons is ConstraintDecorator)) return cons;
                    var nc = (ConstraintDecorator) cons;
                    return nc.Inner;
                })
                .OfType<IAnalysisBasedConstraint>()
                .Select(x => (IAnalyzer<IMetric>) x.Analyzer);
        }
    }
}