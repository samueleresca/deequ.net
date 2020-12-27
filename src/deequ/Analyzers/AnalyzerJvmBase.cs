using System;
using System.Text.RegularExpressions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace deequ.Analyzers
{
    /// <summary>
    /// Analyzer base object to pass and accumulate the analyzers of the run with respect to the JVM
    /// </summary>
    public class AnalyzerJvmBase : IJvmObjectReferenceProvider
    {

        public JvmObjectReference JvmObjectReference;

        protected virtual string AnalyzerName => "";

        protected Func<string, string> AnalyzersNamespaces =
            analyzerName
            => $"com.amazon.deequ.analyzers.{analyzerName}";

        /// <summary>
        /// The target column name subject to the aggregation.
        /// </summary>
        public Option<string> Column;

        /// <summary>
        /// A where clause to filter only some values in a column <see cref="Expr"/>.
        /// </summary>
        public Option<string> Where;

        public AnalyzerJvmBase(JvmObjectReference jvmObjectReference)
        {
            JvmObjectReference = jvmObjectReference;
        }

        public AnalyzerJvmBase(Option<string> column, Option<string> where)
        {
            Column = column;
            Where = where;
        }

        public AnalyzerJvmBase(Option<string> where)
        {
            Where = where;
        }

        public AnalyzerJvmBase()
        {
        }

        public virtual JvmObjectReference Reference {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces(AnalyzerName), Column.ToJvm(), Where.ToJvm());
        }
    }

    public class AnalysisRunBuilder
    {
        /// <summary>
        ///
        /// </summary>
        public SparkSession _sparkSession;
        public DataFrame _df;
        public JvmObjectReference _AnalysisRunBuilder;
        public JvmObjectReference _jvm;

        public AnalysisRunBuilder(SparkSession sparkSession, DataFrame df, JvmObjectReference jvm)
        {
            _sparkSession = sparkSession;
            _df = df;
            _jvm = jvm;
            _AnalysisRunBuilder = jvm.Jvm.CallConstructor("com.amazon.deequ.analyzers.runners.AnalysisRunBuilder", df);

        }

        public AnalysisRunBuilder AddAnalyzer(AnalyzerJvmBase analyzerJvmBase)
        {
            analyzerJvmBase.JvmObjectReference = _jvm;
            _AnalysisRunBuilder.Invoke("addAnalyzer", analyzerJvmBase);
            return this;
        }

        public JvmObjectReference Run()
        {
            return (JvmObjectReference)_AnalysisRunBuilder.Invoke("run");
        }
    }

    public class ApproxCountDistinctJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "ApproxCountDistinctJvm";

        public ApproxCountDistinctJvm(Option<string> column, Option<string> @where) : base(column, @where)
        {
        }
    }


    public class ApproxQuantileJvm : AnalyzerJvmBase
    {
        private float Quantile;
        private float RelativeError;

        public ApproxQuantileJvm(Option<string> column, float quantile, float relativeError = 0.01f, Option<string> where = default)
            : base(column, where)
        {
            Quantile = quantile;
            RelativeError = relativeError;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("ApproxQuantile"), Column.ToJvm(), Quantile, RelativeError, Where.ToJvm());
        }
    }

    public class ApproxQuantilesJvm : AnalyzerJvmBase
    {
        private float[] Quantiles;
        private float RelativeError;

        public ApproxQuantilesJvm(string column, float[] quantiles, float relativeError = 0.01f, Option<string> where = default)
            : base(column, where)
        {
            Quantiles = quantiles;
            RelativeError = relativeError;
        }
        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("ApproxQuantiles"), Column.ToJvm(), Quantiles, RelativeError, Where.ToJvm());
        }
    }

    /// <summary>
    /// Completeness computes the fraction of non-null values ina column of a <see cref="Microsoft.Spark.Sql.DataFrame"/>
    /// </summary>
    public class CompletenessJvm : AnalyzerJvmBase
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="Completeness"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Constraints.Functions.Expr"/>.</param>
        public CompletenessJvm(string column, Option<string> where = default) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Compliance is a measure of the fraction of rows that complies with the given column constraint.
    /// E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value greater than
    /// 3 and 10 rows under 3; a <see cref="DoubleMetric"/> would be returned with 0.33 value
    /// </summary>
    public class ComplianceJvm : AnalyzerJvmBase
    {
        /// <summary>
        /// Describe the compliance.
        /// </summary>
        public string Instance;
        /// <summary>
        /// SQL-Like predicate to apply per row <see cref="Functions.Expr"/>.
        /// </summary>
        public readonly Column Predicate;
        /// <summary>
        /// Initializes a new instance of the <see cref="Compliance"/> class.
        /// </summary>
        /// <param name="instance">Unlike other column analyzers (e.g completeness) this analyzer can not
        ///                      infer to the metric instance name from column name.
        ///                      Also the constraint given here can be referring to multiple columns,
        ///                       so metric instance name should be provided,
        ///                      describing what the analysis being done for.</param>
        /// <param name="predicate">SQL-predicate to apply per row.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Functions.Expr"/>.</param>
        public ComplianceJvm(string instance, Column predicate,  Option<string> where = default) : base(where)
        {
            Predicate = predicate;
            Instance = instance;
            Where = where;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("Compliance"), Instance, Predicate, Where.ToJvm());
        }
    }

    /// <summary>
    /// Computes the pearson correlation coefficient between the two given columns
    /// </summary>
    public class CorrelationJvm : AnalyzerJvmBase
    {
        /// <summary>
        /// First input column for computation.
        /// </summary>
        public readonly string ColumnA;
        /// <summary>
        /// Second input column for computation.
        /// </summary>
        public readonly string ColumnB;

        /// <summary>
        /// Initializes a new instance of the <see cref="Correlation"/> class.
        /// </summary>
        /// <param name="columnA">First input column for computation</param>
        /// <param name="columnB">Second input column for computation.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public CorrelationJvm(string columnA, string columnB, Option<string> where = default) : base(where)
        {
            ColumnA = columnA;
            ColumnB = columnB;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("CorrelationJvm"), ColumnA, ColumnB, Where.ToJvm());
        }

    }


    /// <summary>
    /// Counts the distinct elements in the column(s).
    /// </summary>
    public class CountDistinctJvm : AnalyzerJvmBase
    {
        /// <summary>
        /// Columns to search on.
        /// </summary>
        private readonly string[] Columns;

        /// <summary>
        /// Initializes a new instance of the <see cref="CountDistinctJvm"/> class.
        /// </summary>
        /// <param name="columns">Columns to search on.</param>
        public CountDistinctJvm(string[] columns)
        {
            Columns = columns;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("CountDistinctJvm"), Columns);
        }
    }


    /// <summary>
    /// Data type analyzers, analyzes the data type of the target column.
    /// </summary>
    public class DataTypeJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "DataTypeJvm";
        /// <summary>
        /// Initializes a new instance of the <see cref="DataType"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Functions.Expr"/>.</param>
        public DataTypeJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Distinctness is the fraction of distinct values of a column(s).
    /// </summary>
    public class DistinctnessJvm : AnalyzerJvmBase
    {
        private string[] Columns;
        protected override string AnalyzerName => "DistinctnessJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="Distinctness"/> class.
        /// </summary>
        /// <param name="columns">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public DistinctnessJvm(string[] columns, Option<string> where) : base(where)
        {
            Columns = columns;
        }


        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("DistinctnessJvm"), Columns, Where.ToJvm());
        }
    }


    /// <summary>
    /// Entropy is a measure of the level of information contained in a message. Given the probability
    /// distribution over values in a column, it describes how many bits are required to identify a value.
    /// </summary>
    public class EntropyJvm : AnalyzerJvmBase
    {

        protected override string AnalyzerName => "EntropyJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="Entropy"/> class.
        /// </summary>
        /// <param name="column">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public EntropyJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Computes the max value for the target column.
    /// </summary>
    public class MaximumJvm : AnalyzerJvmBase
    {

        protected override string AnalyzerName => "MaximumJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="Maximum"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public MaximumJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Computes the max value for the target column.
    /// </summary>
    public class MaxLengthJvm : AnalyzerJvmBase
    {

        protected override string AnalyzerName => "MaxLengthJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="MaxLength"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MaxLengthJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Computes the mean for the target column.
    /// </summary>
    public class MeanJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "MeanJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="Mean"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MeanJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Computes the min value for the target column.
    /// </summary>
    public class MinimumJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "MinimumJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="Minimum"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public MinimumJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Computes the min value for the target column.
    /// </summary>
    public class MinLengthJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "MinLengthJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="MinLength"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MinLengthJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Mutual Information describes how much information about one column can be inferred from another
    /// column. If two columns are independent of each other, then nothing can be inferred from one column about
    /// the other, and mutual information is zero. If there is a functional dependency of one column to
    /// another and vice versa, then all information of the two columns are shared, and mutual information is the entropy of each column.
    /// </summary>
    public class MutualInformationJvm : AnalyzerJvmBase
    {
        private string[] Columns;
        protected override string AnalyzerName => "MutualInformationJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="MutualInformation"/> class.
        /// </summary>
        /// <param name="columns">The target column names.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MutualInformationJvm(string[] columns, Option<string> where) : base(where)
        {
            Columns = columns;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("MutualInformationJvm"), Columns, Where.ToJvm());
        }
    }



    /// <summary>
    /// PatternMatch is a measure of the fraction of rows that complies with a given column regex constraint.
    /// E.g if the constraint is Patterns.CREDITCARD and the data frame has 5 rows which contain a credit card number in a certain column
    /// according to the regex and and 10 rows that do not, a DoubleMetric would be
    /// returned with 0.33 as value
    /// </summary>
    public class PatternMatchJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "PatternMatchJvm";
        /// <summary>
        /// Column to do the pattern match analysis on.
        /// </summary>
        public readonly Regex Regex;

        /// <summary>
        /// Initializes a new instance of type <see cref="PatternMatch"/> class.
        /// </summary>
        /// <param name="column">Column to do the pattern match analysis on.</param>
        /// <param name="regex">The regular expression to check for.</param>
        /// <param name="where">Additional filter to apply before the analyzer is run.</param>
        public PatternMatchJvm(string column, Regex regex, Option<string> where) : base(column, where)
        {
            Regex = regex;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("PatternMatchJvm"),
                    Column.ToJvm(),
                    JvmObjectReference.Jvm.CallConstructor("scala.util.matching.Regex",Regex),
                    Where.ToJvm());
        }
    }


    /// <summary>
    /// Size computes the number of rows in a <see cref="DataFrame"/>.
    /// </summary>
    public class SizeJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "SizeJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="Size"/> class.
        /// </summary>
        /// <param name="where">Additional filter to apply before the analyzer is run.</param>
        public SizeJvm(Option<string> where) : base(where)
        {
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces("SizeJvm"),
                    Where.ToJvm());
        }
    }

    /// <summary>
    /// Computes the standard deviation of a column.
    /// </summary>
    public class StandardDeviationJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "StandardDeviationJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="StandardDeviation"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public StandardDeviationJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Computes the sum of data.
    /// </summary>
    public class SumJvm : AnalyzerJvmBase
    {
        protected override string AnalyzerName => "SumJvm";

        /// <summary>
        /// Initializes a new instance of type <see cref="Sum"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public SumJvm(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Uniqueness is the fraction of unique values of a column(s), i.e., values that occur exactly once.
    /// </summary>
    public class UniquenessJvm : AnalyzerJvmBase
    {
        private string[] Columns;
        protected override string AnalyzerName => "Uniqueness";

        /// <summary>
        /// Initializes a new instance of type <see cref="Uniqueness"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public UniquenessJvm(string[] columns, Option<string> where) : base( where)
        {
            Columns = columns;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces(AnalyzerName),
                    Columns,
                    Where.ToJvm());
        }
    }

    /// <summary>
    /// Computes the unique value ratio of a <see cref="DataFrame"/> column.
    /// </summary>
    public class UniqueValueRatioJvm : AnalyzerJvmBase
    {
        private string[] Columns;
        protected override string AnalyzerName => "UniqueValueRatio";

        /// <summary>
        /// Initializes a new instance of type <see cref="Uniqueness"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public UniqueValueRatioJvm(string[] columns, Option<string> where) : base( where)
        {
            Columns = columns;
        }

        public override JvmObjectReference Reference
        {
            get => JvmObjectReference
                .Jvm.CallConstructor(
                    AnalyzersNamespaces(AnalyzerName),
                    Columns,
                    Where.ToJvm());
        }
    }



}
