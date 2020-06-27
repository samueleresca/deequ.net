using System;
using System.Collections.Generic;
using System.Linq;
using xdeequ.Constraints;
using xdeequ.Util;

namespace xdeequ.Checks
{
    public class CheckWithLastConstraintFilterable : Check
    {
        public CheckWithLastConstraintFilterable(CheckLevel level, string description,
            IEnumerable<IConstraint> constraints, Func<Option<string>, IConstraint> creationFunc)
            : base(level, description, constraints) =>
            CreateReplacement = creationFunc;

        public Func<Option<string>, IConstraint> CreateReplacement { get; set; }

        public Check Where(string filter)
        {
            IEnumerable<IConstraint> adjustedConstraints =
                Constraints.Take(Constraints.Count() - 1).Append(CreateReplacement(new Option<string>(filter)));

            return new Check(Level, Description, adjustedConstraints);
        }
    }
}
