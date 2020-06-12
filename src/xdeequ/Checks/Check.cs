#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using xdeequ.Constraints;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static xdeequ.Constraints.Functions;

namespace xdeequ.Checks
{
    public class CheckResult
    {
        public Check Check { get; set; }
        public CheckStatus Status { get; set; }
        public CheckResult[] ConstraintResults { get; set; }
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
        public CheckLevel Level { get; set; }
        public string Description { get; set; }
        protected IEnumerable<IConstraint> Constraints { get; set; }


        private static Func<double, bool> IsOne = new Func<double, bool>(_ => _ == 1.0);

        public Check(CheckLevel level, string description, IEnumerable<IConstraint> constraints)
        {
            Level = level;
            Description = description;
            Constraints = constraints;
        }

        private CheckWithLastConstraintFilterable AddFilterableConstraint(
            Func<string, IConstraint> constraintDefinition)
        {
            var constraintWithoutFiltering = constraintDefinition(string.Empty);
            var newConstraints = Constraints.Append(constraintWithoutFiltering);

            return new CheckWithLastConstraintFilterable(Level, Description, newConstraints);
        }

        public Check AddConstraint(IConstraint constraint)
        {
            Constraints = Constraints.Append(constraint);
            return this;
        }

        public CheckWithLastConstraintFilterable HasSize(Func<long, bool> assertion, Option<string> hint)
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

        public CheckWithLastConstraintFilterable Satisfies(string columnCondition, string constraintName,
            Func<double, bool> assertion, Option<string> hint)
        {
            return AddFilterableConstraint(filter =>
                ComplianceConstraint(constraintName, columnCondition, assertion, filter, hint));
        }
    }
}