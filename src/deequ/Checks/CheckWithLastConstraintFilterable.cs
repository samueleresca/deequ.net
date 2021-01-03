using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Constraints;
using deequ.Util;

namespace deequ.Checks
{
    /// <summary>
    /// Allows to replace the last configured constraint in a check with a filtered version
    /// </summary>
    public class CheckWithLastConstraintFilterable : Check
    {
        /// <summary>
        /// Constructor of <see cref="CheckWithLastConstraintFilterable"/> class>
        /// </summary>
        /// <param name="level">Check level</param>
        /// <param name="description">Check description</param>
        /// <param name="constraints">Constraints to run</param>
        /// <param name="creationFunc">Creation function</param>
        public CheckWithLastConstraintFilterable(CheckLevel level, string description,
            IEnumerable<IConstraint> constraints, Func<Option<string>, IConstraint> creationFunc)
            : base(level, description, constraints) =>
            CreateReplacement = creationFunc;

        private Func<Option<string>, IConstraint> CreateReplacement { get; }

        /// <summary>
        /// Defines a filter to apply before evaluating the previous constraint
        /// </summary>
        /// <param name="filter">SparkSQL predicate to apply</param>
        /// <returns></returns>
        public Check Where(string filter)
        {
            IEnumerable<IConstraint> adjustedConstraints =
                Constraints.Take(Constraints.Count() - 1).Append(CreateReplacement(filter));

            return new Check(Level, Description, adjustedConstraints);
        }
    }
}
