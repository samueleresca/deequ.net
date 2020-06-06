#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;
using xdeequ.Util;
using Constraint = xdeequ.Constraints.Constraint;

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
        protected IEnumerable<Constraint> Constraints { get; set; }

        public Check(CheckLevel level, string description, IEnumerable<Constraint> constraints)
        {
            Level = level;
            Description = description;
            Constraints = constraints;
        }

        private CheckWithLastConstraintFilterable AddFilterableConstraint(Func<string, Constraint> constraintDefinition)
        {
            var constraintWithoutFiltering = constraintDefinition(string.Empty);
            var newConstraints = Constraints.Append(constraintWithoutFiltering);

            return new CheckWithLastConstraintFilterable(Level, Description, newConstraints);
        }

        public Check AddConstraint(Constraint constraint)
        {
            Constraints = Constraints.Append(constraint);
            return this;
        }

        public CheckWithLastConstraintFilterable HasSize(Func<long, bool> assertion, Option<string> hint)
        {
            // AddFilterableConstraint(filter => Analyzers.Siz(assertion, filter, hint));
            return new CheckWithLastConstraintFilterable(new CheckLevel(), null, null);
        }
    }
}