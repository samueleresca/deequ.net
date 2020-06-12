using System;
using System.Collections.Generic;
using System.Linq;
using xdeequ.Constraints;

namespace xdeequ.Checks
{
    public class CheckWithLastConstraintFilterable : Check
    {
        public Func<string, IConstraint> CreateReplacement { get; set; }

        public CheckWithLastConstraintFilterable(CheckLevel level, string description,
            IEnumerable<IConstraint> constraints)
            : base(level, description, constraints)
        {
        }

        public Check Where(string filter)
        {
            var adjustedConstraints =
                Constraints.Take(Constraints.Count() - 1).Append(CreateReplacement(filter));

            return new Check(Level, Description, adjustedConstraints);
        }
    }
}