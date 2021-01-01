using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using deequ.Analyzers;
using deequ.Analyzers.States;
using deequ.AnomalyDetection;
using deequ.Constraints;
using deequ.Metrics;
using deequ.Repository;
using deequ.Util;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using static deequ.Constraints.Functions;
using static Microsoft.Spark.Sql.Functions;

namespace deequ.Checks
{
    /// <summary>
    /// A class representing a list of constraints that can be applied to a given
    /// <see cref="DataFrame"/>. In order to run the checks, use the `run` method. You can
    /// also use VerificationSuite.run to run your checks along with other Checks and Analysis objects.
    /// When run with VerificationSuite, Analyzers required by multiple checks/analysis blocks is
    /// optimized to run once.
    /// </summary>
    public class Check
    {
        /// <summary>
        ///
        /// </summary>
        public static readonly Func<double, bool> IsOne = val => val == 1.0;

        /// <summary>
        ///
        /// </summary>
        public CheckLevel Level { get; }
        /// <summary>
        ///
        /// </summary>
        public string Description { get; }
        /// <summary>
        ///
        /// </summary>
        public IEnumerable<IConstraint> Constraints { get; set; }

        /// <summary>
        /// Constructor of class <see cref="Check"/>
        /// </summary>
        /// <param name="level">Assertion level of the check group of type <see cref="CheckLevel"/> . If any of the constraints fail this level is used for the status of the check.</param>
        /// <param name="description">The name describes the check block. Generally will be used to show in the logs.</param>
        /// <param name="constraints">The constraints list of type <see cref="IConstraint"/> to apply when this check is run.</param>
        public Check(CheckLevel level, string description, IEnumerable<IConstraint> constraints)
        {
            Level = level;
            Description = description;
            Constraints = constraints;
        }

        /// <summary>
        /// Constructor of class <see cref="Check"/>
        /// </summary>
        /// <param name="level">Assertion level of the check group of type <see cref="CheckLevel"/> . If any of the constraints fail this level is used for the status of the check.</param>
        /// <param name="description">The name describes the check block. Generally will be used to show in the logs.</param>
        public Check(CheckLevel level, string description)
        {
            Level = level;
            Description = description;
            Constraints = new List<IConstraint>();
        }

        /// <summary>
        /// Constructor of class <see cref="Check"/>
        /// </summary>
        /// <param name="level">Assertion level of the check group of type <see cref="CheckLevel"/> . If any of the constraints fail this level is used for the status of the check.</param>
        public Check(CheckLevel level)
        {
            Level = level;
            Constraints = new List<IConstraint>();
        }

        /// <summary>
        /// Returns a new Check instance with the given constraint added to the constraints list.
        /// </summary>
        /// <param name="constraint">The constraint to add of type <see cref="IConstraint"/>.</param>
        /// <returns>The new constraint instance.</returns>
        public Check AddConstraint(IConstraint constraint)
        {
            Constraints = Constraints.Append(constraint);
            return this;
        }
        /// <summary>
        /// Creates a constraint that calculates the data frame size and runs the assertion on it.
        /// </summary>
        /// <param name="assertion">Function that receives a long input parameter and returns a boolean
        ///                  Assertion functions might refer to the data frame size by "_"
        ///                  <code>.HasSize(value => value > 5)</code> meaning the number of rows should be greater than 5
        ///                  Or more elaborate function might be provided
        ///                  <code>.HasSize(aNameForSize => aNmeForSize > 0 &amp;&amp; aNmeForSize &lt; 10 </code></param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasSize(Func<double, bool> assertion, Option<string> hint = default) =>
            AddFilterableConstraint(filter => SizeConstraint(assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on a column completion.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsComplete(string column, Option<string> hint = default) =>
            AddFilterableConstraint(filter => CompletenessConstraint(column, IsOne, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on a column completion.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasCompleteness(string column, Func<double, bool> assertion,
            Option<string> hint = default) =>
            AddFilterableConstraint(filter => CompletenessConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on completion in combined set of columns.
        /// </summary>
        /// <param name="columns">Columns to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable AreComplete(IEnumerable<string> columns, Option<string> hint = default) =>
            Satisfies(ChecksExt.IsEachNotNull(columns), "Combined Completeness", IsOne, hint);

        /// <summary>
        /// Creates a constraint that assert on completion in combined set of columns.
        /// </summary>
        /// <param name="columns">Columns to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HaveCompleteness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint = default) =>
            Satisfies(ChecksExt.IsEachNotNull(columns), "Combined Completeness", assertion, hint);

        /// <summary>
        /// Creates a constraint that asserts on completion in combined set of columns.
        /// </summary>
        /// <param name="columns">Columns to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable AreAnyComplete(IEnumerable<string> columns, Option<string> hint = default) =>
            Satisfies(ChecksExt.IsAnyNotNull(columns), "Any Completeness", IsOne, hint);

        /// <summary>
        /// Creates a constraint that assert on completion in combined set of columns.
        /// </summary>
        /// <param name="columns">Columns to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HaveAnyCompleteness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint = default) =>
            Satisfies(ChecksExt.IsAnyNotNull(columns), "Any Completeness", assertion, hint);

        /// <summary>
        /// Creates a constraint that asserts on a column uniqueness.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsUnique(string column, Option<string> hint = default) =>
            AddFilterableConstraint(filter => UniquenessConstraint(column, IsOne, filter, hint));


        /// <summary>
        /// Creates a constraint that asserts on a columns uniqueness.
        /// </summary>
        /// <param name="columns">Key columns.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable AreUnique(IEnumerable<string> columns, Option<string> hint = default) =>
            AddFilterableConstraint(filter => UniquenessConstraint(columns, IsOne, filter, hint));


        /// <summary>
        /// Creates a constraint that asserts on a column(s) primary key characteristics. Currently only checks uniqueness, but reserved for primary key checks if there is another assertion to run on primary key columns.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="columns">Columns to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsPrimaryKey(string column,
            IEnumerable<string> columns, Option<string> hint = default) =>
            AddFilterableConstraint(filter =>
                UniquenessConstraint(new[] { column }.Concat(columns), IsOne, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on uniqueness in a single or combined set of key columns.
        /// </summary>
        /// <param name="columns">Key columns.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean. Refers to the fraction of unique values</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasUniqueness(IEnumerable<string> columns,
            Func<double, bool> assertion, Option<string> hint = default) =>
            AddFilterableConstraint(filter => UniquenessConstraint(columns, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the uniqueness of a key column.
        /// </summary>
        /// <param name="column">Key column</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean. Refers to the fraction of unique values.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasUniqueness(string column, Func<double, bool> assertion,
            Option<string> hint = default) =>
            AddFilterableConstraint(filter => UniquenessConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the uniqueness of a key column.
        /// </summary>
        /// <param name="columns">Key columns.</param>
        /// <param name="assertion">Creates a constraint that asserts on the uniqueness of a key column.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasDistinctness(IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> hint = default) =>
            AddFilterableConstraint(filter => DistinctnessConstraint(columns, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint on the unique value ratio in a single or combined set of key columns.
        /// </summary>
        /// <param name="columns">columns</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean. Refers to the fraction of distinct values.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasUniqueValueRatio(IEnumerable<string> columns,
            Func<double, bool> assertion,
            Option<string> hint = default) =>
            AddFilterableConstraint(filter => UniqueValueRatioConstraint(columns, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the number of distinct values a column has.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a long input parameter and returns a boolean.</param>
        /// <param name="binningFunc">An optional binning function.</param>
        /// <param name="maxBins">Histogram details is only provided for N column values with top counts. maxBins sets the N</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasNumberOfDistinctValues(string column,
            Func<long, bool> assertion,
            Option<UserDefinedFunction> binningFunc = default,
            int maxBins = 1000,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter =>
                HistogramBinConstraint(column, assertion, binningFunc, filter, hint, maxBins));


        /// <summary>
        /// Creates a constraint that asserts on column's value distribution.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a Distribution input parameter and returns a boolean. E.g <code>.hasHistogramValues("att2", _.absolutes("f") == 3 .hasHistogramValues("att2",_.ratios(Histogram.NullFieldReplacement) == 2/6.0)</code></param>
        /// <param name="binningFunc">An optional binning function.</param>
        /// <param name="maxBins">Histogram details is only provided for N column values with top counts. maxBins sets the N.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasHistogramValues(string column,
            Func<Distribution, bool> assertion,
            Option<UserDefinedFunction> binningFunc = default,
            int maxBins = 1000,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter =>
                HistogramConstraint(column, assertion, binningFunc, filter, hint, maxBins));

        public CheckWithLastConstraintFilterable KllSketchSatisfies(string column,
            Func<Distribution, bool> assertion,
            Option<Func<Column, Column>> binningFunc = default,
            Option<string> hint = default,
            int maxBins = 100
        ) =>
            throw new NotImplementedException();

        /// <summary>
        /// Creates a constraint that runs <see cref="IAnomalyDetectionStrategy"/> on the new value
        /// </summary>
        /// <param name="metricRepository">A metrics repository to get the previous results.</param>
        /// <param name="anomalyDetectionStrategy">The anomaly detection strategy.</param>
        /// <param name="analyzer">The analyzer for the metric to run anomaly detection on.</param>
        /// <param name="withTagValues">Can contain a Map with tag names and the corresponding values to filter for.</param>
        /// <param name="afterDate">The maximum DateTime of previous <see cref="AnalysisResults"/> to use for the Anomaly Detection.</param>
        /// <param name="beforeDate">The minumum DateTime of previous <see cref="AnalysisResults"/> to use for the Anomaly Detection.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <typeparam name="S"><see cref="IState"/></typeparam>
        /// <returns></returns>
        public Check IsNewestPointNonAnomalous<S>(
            IMetricsRepository metricRepository,
            IAnomalyDetectionStrategy anomalyDetectionStrategy,
            IAnalyzer<IMetric> analyzer,
            Dictionary<string, string> withTagValues,
            Option<long> afterDate = default,
            Option<long> beforeDate = default,
            Option<string> hint = default
        ) where S : IState
        {
            Func<double, bool> funcResult = IsNewestPointNonAnomalous(metricRepository, anomalyDetectionStrategy,
                withTagValues,
                afterDate,
                beforeDate, analyzer);

            return AddConstraint(AnomalyConstraint<S>(analyzer, funcResult, Option<string>.None));
        }

        /// <summary>
        /// Creates a constraint that asserts on a column entropy.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasEntropy(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => EntropyConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on a mutual information between two columns.
        /// </summary>
        /// <param name="columnA">First column for mutual information calculation.</param>
        /// <param name="columnB">Second column for mutual information calculation.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasMutualInformation(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter =>
                MutualInformationConstraint(columnA, columnB, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on an approximated quantile.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="quantile">Which quantile to assert on.</param>
        /// <param name="assertion">Function that receives a double input parameter (the computed quantile) and returns a boolean</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasApproxQuantile(
            string column,
            double quantile,
            Func<double, bool> assertion,
            Option<string> hint
        ) =>
            throw new NotImplementedException();

        /// <summary>
        /// Creates a constraint that asserts on the minimum length of the column.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasMinLength(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => MinLengthConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the maximum length of the column.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasMaxLength(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => MaxLengthConstraint(column, assertion, filter, hint));


        /// <summary>
        /// Creates a constraint that asserts on the minimum of the column.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasMin(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => MinConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the maximum of the column.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasMax(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => MaxConstraint(column, assertion, filter, hint));


        /// <summary>
        /// Creates a constraint that asserts on the mean of the column.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasMean(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => MeanConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the sum of the column.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasSum(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => SumConstraint(column, assertion, filter, hint));


        /// <summary>
        /// Creates a constraint that asserts on the standard deviation of the column
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasStandardDeviation(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => StandardDeviationConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the approximate count distinct of the given column
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasApproxCountDistinct(string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => ApproxCountDistinctConstraint(column, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that asserts on the pearson correlation between two columns.
        /// </summary>
        /// <param name="columnA">First column for correlation calculation.</param>
        /// <param name="columnB">Second column for correlation calculation.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasCorrelation(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => CorrelationConstraint(columnA, columnB, assertion, filter, hint));


        /// <summary>
        /// Creates a constraint that runs the given condition on the data frame.
        /// </summary>
        /// <param name="columnCondition">Data frame column which is a combination of expression and the column name. It has to comply with Spark SQL syntax. Can be written in an exact same way with conditions inside the `WHERE` clause.</param>
        /// <param name="constraintName">A name that summarizes the check being made. This name is being used to name the metrics for the analysis being done.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable Satisfies(string columnCondition, string constraintName,
            Func<double, bool> assertion, Option<string> hint = default) =>
            Satisfies(Expr(columnCondition), constraintName, assertion, hint);


        /// <summary>
        /// Creates a constraint that runs the given condition on the data frame.
        /// </summary>
        /// <param name="columnCondition">Data frame column which is a combination of expression and the column name. It has to comply with Spark SQL syntax. Can be written in an exact same way with conditions inside the `WHERE` clause.</param>
        /// <param name="constraintName">A name that summarizes the check being made. This name is being used to name the metrics for the analysis being done.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable Satisfies(
            string columnCondition,
            string constraintName,
            Option<string> hint = default) => Satisfies(Expr(columnCondition), constraintName, hint);


        /// <summary>
        /// Creates a constraint that runs the given condition on the data frame.
        /// </summary>
        /// <param name="columnCondition">Data frame column which is a combination of expression and the column name. It has to comply with Spark SQL syntax. Can be written in an exact same way with conditions inside the `WHERE` clause.</param>
        /// <param name="constraintName">A name that summarizes the check being made. This name is being used to name the metrics for the analysis being done.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable Satisfies(Column columnCondition, string constraintName,
            Func<double, bool> assertion, Option<string> hint = default) =>
            AddFilterableConstraint(filter =>
                ComplianceConstraint(constraintName, columnCondition, assertion, filter, hint));

        /// <summary>
        /// Creates a constraint that runs the given condition on the data frame.
        /// </summary>
        /// <param name="columnCondition">Data frame column which is a combination of expression and the column name. It has to comply with Spark SQL syntax. Can be written in an exact same way with conditions inside the `WHERE` clause.</param>
        /// <param name="constraintName">A name that summarizes the check being made. This name is being used to name the metrics for the analysis being done.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable Satisfies(Column columnCondition, string constraintName,
            Option<string> hint = default) =>
            AddFilterableConstraint(filter =>
                ComplianceConstraint(constraintName, columnCondition, IsOne, filter, hint));

        /// <summary>
        /// Checks for pattern compliance. Given a column name and a regular expression, defines a Check on the average compliance of the column's values to the regular expression.
        /// </summary>
        /// <param name="column">Name of the column that should be checked.</param>
        /// <param name="pattern">The columns values will be checked for a match against this pattern.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="name">A name that summarizes the check being made. This name is being used to name the metrics for the analysis being done.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasPattern(
            string column,
            Regex pattern,
            Func<double, bool> assertion,
            Option<string> name = default,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter =>
                PatternMatchConstraint(column, pattern, assertion, filter, hint: hint));

        /// <summary>
        /// Checks for pattern compliance. Given a column name and a regular expression, defines a Check on the average compliance of the column's values to the regular expression.
        /// </summary>
        /// <param name="column">Name of the column that should be checked.</param>
        /// <param name="pattern">The columns values will be checked for a match against this pattern.</param>
        /// <param name="name">A name that summarizes the check being made. This name is being used to name the metrics for the analysis being done.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasPattern(
            string column,
            Regex pattern,
            Option<string> name = default,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter =>
                PatternMatchConstraint(column, pattern, IsOne, filter, name, hint));

        /// <summary>
        /// Check to run against the compliance of a column against an credit-card pattern.
        /// </summary>
        /// <param name="column">Name of the column that should be checked.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable ContainsCreditCardNumber(
            string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            HasPattern(column, Patterns.CreditCard, assertion, $"ContainsCreditCardNumber({column})", hint);


        /// <summary>
        /// Check to run against the compliance of a column against an email pattern.
        /// </summary>
        /// <param name="column">Name of the column that should be checked.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable ContainsEmail(
            string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            HasPattern(column, Patterns.Email, assertion, $"ContainsEmail({column})", hint);

        /// <summary>
        /// Check to run against the compliance of a column against an URL pattern.
        /// </summary>
        /// <param name="column">Name of the column that should be checked.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable ContainsURL(
            string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            HasPattern(column, Patterns.Url, assertion, $"ContainsURL({column})", hint);


        /// <summary>
        /// Check to run against the compliance of a column against an SSN pattern.
        /// </summary>
        /// <param name="column">Name of the column that should be checked.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable ContainsSSN(
            string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            HasPattern(column, Patterns.SocialSecurityNumberUs, assertion, $"ContainsSSN({column})", hint);

        /// <summary>
        /// Check to run against the fraction of rows that conform to the given data type.
        /// </summary>
        /// <param name="column">Name of the column that should be checked.</param>
        /// <param name="dataType">Data type to verify <see cref="ConstrainableDataTypes"/></param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable HasDataType(
            string column,
            ConstrainableDataTypes dataType,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            AddFilterableConstraint(filter => DataTypeConstraint(column, dataType, assertion, filter, hint));


        /// <summary>
        /// Creates a constraint that asserts that a column contains no negative values.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsNonNegative(
            string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"COALESCE({column}, 0.0) >= 0"), $"{column} is non-negative", assertion, hint);


        /// <summary>
        /// Creates a constraint that asserts that a column contains no negative values.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsNonNegative(
            string column,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"COALESCE({column}, 0.0) >= 0"), $"{column} is non-negative", hint);


        /// <summary>
        /// Creates a constraint that asserts that a column contains no negative values.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsPositive(
            string column,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"COALESCE({column}, 1.0) >= 0"), $"{column} is positive", assertion, hint);


        /// <summary>
        /// Creates a constraint that asserts that a column contains no negative values.
        /// </summary>
        /// <param name="column">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsPositive(
            string column,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"COALESCE({column}, 1.0) >= 0"), $"{column} is positive", hint);



        /// <summary>
        /// Asserts that, in each row, the value of columnA is less than the value of columnB
        /// </summary>
        /// <param name="columnA">Column to run the assertion on.</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsLessThan(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} < {columnB}"), $"{columnA} is less than {columnB}", assertion, hint);

        /// <summary>
        /// Asserts that, in each row, the value of columnA is less than the value of columnB
        /// </summary>
        /// <param name="columnA">Column to run the assertion on.</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsLessThan(
            string columnA,
            string columnB,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} < {columnB}"), $"{columnA} is less than {columnB}", hint);

        /// <summary>
        /// Asserts that, in each row, the value of columnA is less than or equal to the value of columnB.
        /// </summary>
        /// <param name="columnA">Column to run the assertion on</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsLessThanOrEqualTo(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} <= {columnB}"), $"{columnA} is less than or equal to {columnB}",
                assertion,
                hint);


        /// <summary>
        /// Asserts that, in each row, the value of columnA is less than or equal to the value of columnB.
        /// </summary>
        /// <param name="columnA">Column to run the assertion on</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsLessThanOrEqualTo(
            string columnA,
            string columnB,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} <= {columnB}"), $"{columnA} is less than or equal to {columnB}",
                hint);

        /// <summary>
        /// Asserts that, in each row, the value of columnA is greater than the value of columnB.
        /// </summary>
        /// <param name="columnA">Column to run the assertion on</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsGreaterThan(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} > {columnB}"), $"{columnA} is greater than {columnB}", assertion, hint);

        /// <summary>
        /// Asserts that, in each row, the value of columnA is greater than the value of columnB.
        /// </summary>
        /// <param name="columnA">Column to run the assertion on</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsGreaterThan(
            string columnA,
            string columnB,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} > {columnB}"), $"{columnA} is greater than {columnB}", hint);

        /// <summary>
        /// Asserts that, in each row, the value of columnA is greater than or equal to the value of columnB.
        /// </summary>
        /// <param name="columnA">Column to run the assertion on</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsGreaterOrEqualTo(
            string columnA,
            string columnB,
            Func<double, bool> assertion,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} >= {columnB}"), $"{columnA} is greater than or equal to {columnB}",
                assertion,
                hint);

        /// <summary>
        /// Asserts that, in each row, the value of columnA is greater than or equal to the value of columnB.
        /// </summary>
        /// <param name="columnA">Column to run the assertion on</param>
        /// <param name="columnB">Column to run the assertion on.</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsGreaterOrEqualTo(
            string columnA,
            string columnB,
            Option<string> hint = default
        ) =>
            Satisfies(Expr($"{columnA} >= {columnB}"), $"{columnA} is greater than or equal to {columnB}",
                hint);

        /// <summary>
        /// Asserts that every non-null value in a column is contained in a set of predefined values
        /// </summary>
        /// <param name="column">Column to run the assertion on</param>
        /// <param name="allowedValues">allowed values for the column</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Option<string> hint = default
        ) =>
            IsContainedIn(column, allowedValues, IsOne, hint);

        /// <summary>
        /// Asserts that every non-null value in a column is contained in a set of predefined values
        /// </summary>
        /// <param name="column">Column to run the assertion on</param>
        /// <param name="allowedValues">allowed values for the column</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Func<double, bool> assertion
        ) =>
            IsContainedIn(column, allowedValues, assertion, Option<string>.None);

        /// <summary>
        /// Asserts that every non-null value in a column is contained in a set of predefined values
        /// </summary>
        /// <param name="column">Column to run the assertion on</param>
        /// <param name="lowerBound">lower bound of the interval.</param>
        /// <param name="upperBound">upper bound of the interval.</param>
        /// <param name="includeUpperBound">is a value equal to the lower bound allows?</param>
        /// <param name="includeLowerBound">is a value equal to the upper bound allowed?</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            double lowerBound,
            double upperBound,
            bool includeUpperBound = true,
            bool includeLowerBound = true,
            Option<string> hint = default
        )
        {
            string leftOperand = includeLowerBound ? ">=" : ">";
            string rightOperand = includeUpperBound ? "<=" : "<";

            string predictate = $"{column} IS NULL OR" +
                                $"(`{column}` {leftOperand} {lowerBound} AND {column} {rightOperand} {upperBound} )";

            return Satisfies(Expr(predictate), $"{column} between {lowerBound} and {upperBound}", hint);
        }

        /// <summary>
        /// Asserts that every non-null value in a column is contained in a set of predefined values
        /// </summary>
        /// <param name="column">Column to run the assertion on</param>
        /// <param name="allowedValues">allowed values for the column</param>
        /// <param name="assertion">Function that receives a double input parameter and returns a boolean</param>
        /// <param name="hint">A hint to provide additional context why a constraint could have failed.</param>
        /// <returns></returns>
        public CheckWithLastConstraintFilterable IsContainedIn(
            string column,
            IEnumerable<string> allowedValues,
            Func<double, bool> assertion,
            Option<string> hint = default
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

        /// <summary>
        /// Evaluate this check on computed metrics.
        /// </summary>
        /// <param name="context">result of the metrics computation.</param>
        /// <returns></returns>
        public CheckResult Evaluate(AnalyzerContext context)
        {
            IEnumerable<ConstraintResult> constraintResults = Constraints.Select(constraint =>
                constraint.Evaluate((IJvmObjectReferenceProvider)context.MetricMap()));
            bool anyFailure = constraintResults.Any(constraintResult => constraintResult.Status == ConstraintStatus.Failure);

            CheckStatus checkStatus = (anyFailure, Level) switch
            {
                (true, CheckLevel.Error) => CheckStatus.Error,
                (true, CheckLevel.Warning) => CheckStatus.Warning,
                _ => CheckStatus.Success
            };

            return new CheckResult(this, checkStatus, constraintResults);
        }

        /// <summary>
        /// Returns the required analyzers
        /// </summary>
        /// <returns></returns>
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
                .Select(constraint => constraint.Analyzer);

        private CheckWithLastConstraintFilterable AddFilterableConstraint(
            Func<Option<string>, IConstraint> constraintDefinition)
        {
            IConstraint constraintWithoutFiltering = constraintDefinition(Option<string>.None);
            IEnumerable<IConstraint> newConstraints = Constraints.Append(constraintWithoutFiltering);

            return new CheckWithLastConstraintFilterable(Level, Description, newConstraints, constraintDefinition);
        }

        private Func<double, bool> IsNewestPointNonAnomalous(IMetricsRepository metricsRepository,
            IAnomalyDetectionStrategy anomalyDetectionStrategy,
            Option<Dictionary<string, string>> withTagValues,
            Option<long> valueAfterDate,
            Option<long> valueBeforeDate,
            IAnalyzer<IMetric> analyzer)
        {
            return d =>
            {
                // Get history keys
                IMetricRepositoryMultipleResultsLoader repositoryLoader = metricsRepository.Load();

                withTagValues.OnSuccess(value => { repositoryLoader = repositoryLoader.WithTagValues(value); });


                valueBeforeDate.OnSuccess(value => { repositoryLoader = repositoryLoader.Before(value); });

                valueAfterDate.OnSuccess(value => { repositoryLoader = repositoryLoader.After(value); });

                repositoryLoader = repositoryLoader.ForAnalyzers(new[] { analyzer });

                IEnumerable<AnalysisResult> analysisResults = repositoryLoader.Get();

                if (!analysisResults.Any())
                {
                    throw new ArgumentException("There have to be previous results in the MetricsRepository!");
                }


                IEnumerable<(long dataSetDate, Option<Metric<double>> doubleMetricOption)> historicalMetrics =
                    analysisResults
                        //TODO: Order by tags in case you have multiple data points .OrderBy(x => x.ResultKey.Tags.Values)
                        .Select(analysisResults =>
                        {
                            JvmObjectReference analyzerContextMetricMap = analysisResults.AnalyzerContext.MetricMap();
                            Metric<double> doubleMetric = (Metric<double>) analyzerContextMetricMap
                                .Invoke("headOption.get._2");

                            Option<Metric<double>> doubleMetricOption = Option<Metric<double>>.None;

                            if (doubleMetric != null)
                            {
                                doubleMetricOption = new Option<Metric<double>>(doubleMetric);
                            }


                            long dataSetDate = analysisResults.ResultKey.DataSetDate;

                            return (dataSetDate, doubleMetricOption);
                        });

                long testDateTime = analysisResults.Select(analysisResult => analysisResult.ResultKey.DataSetDate).Max() + 1;

                if (testDateTime == long.MaxValue)
                {
                    throw new ArgumentException("Test DateTime cannot be Long.MaxValue, otherwise the" +
                                                "Anomaly Detection, which works with an open upper interval bound, won't test anything");
                }

                AnomalyDetector anomalyDetector = new AnomalyDetector(anomalyDetectionStrategy);
                IEnumerable<DataPoint<double>> metricsOptions = historicalMetrics
                    .Select(pair =>
                    {
                        Option<double> valueOption = Option<double>.None;

                        if (pair.doubleMetricOption.HasValue && pair.doubleMetricOption.Value.IsSuccess())
                        {
                            valueOption = new Option<double>(pair.doubleMetricOption.Value.Value.Get());
                        }

                        return new DataPoint<double>(pair.dataSetDate, valueOption);
                    });


                DetectionResult detectedAnomalies = anomalyDetector.IsNewPointAnomalous(metricsOptions,
                    new DataPoint<double>(testDateTime, new Option<double>(d)));

                return !detectedAnomalies.Anomalies.Any();
            };
        }
    }
}
