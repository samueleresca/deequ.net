using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using deequ.Interop.Utils;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;

namespace deequ.Analyzers
{
    /// <summary>
    /// Analyzer base object to pass and accumulate the analyzers of the run with respect to the JVM
    /// </summary>
    public class AnalyzerJvmBase : IJvmObjectReferenceProvider, IAnalyzer<IMetric>
    {
        protected Option<JvmObjectReference> _jvmBridge = Option<JvmObjectReference>.None;

        protected string AnalyzerName => GetType().Name;

        protected readonly Func<string, string> AnalyzersNamespaces =
            analyzerName
            => $"com.amazon.deequ.analyzers.{analyzerName}";

        public AnalyzerJvmBase(Option<string> column = default, Option<string> where = default)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName),
                column.Value,
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$2")));
        }

        public AnalyzerJvmBase(Option<string> where): this(Option<string>.None, where)
        {
        }

        public AnalyzerJvmBase(JvmObjectReference jvmObjectReference)
        {
            _jvmBridge = jvmObjectReference;
        }

        public AnalyzerJvmBase()
        {

        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="jvmObjectReference"></param>
        /// <returns></returns>
        public static implicit operator AnalyzerJvmBase(JvmObjectReference jvmObjectReference)
        {
            return new AnalyzerJvmBase(jvmObjectReference);
        }



        /// <summary>
        ///
        /// </summary>
        public JvmObjectReference Reference
        {
            get
            {
                return _jvmBridge.GetOrElse(null);
            }
        }

        public override string ToString() => (string) _jvmBridge.Value.Invoke("toString");
    }


    public class ApproxCountDistinct : AnalyzerJvmBase
    {
        public ApproxCountDistinct(Option<string> column, Option<string> @where) : base(column, @where)
        {
        }
    }


    public class ApproxQuantile : AnalyzerJvmBase
    {

        public ApproxQuantile(Option<string> column, float quantile, float relativeError = 0.01f, Option<string> where = default)
        {
            _jvmBridge =  SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName), column, quantile, relativeError,
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$4")));
        }
    }

    public class ApproxQuantiles : AnalyzerJvmBase
    {
        public ApproxQuantiles(Option<string> column, float[] quantiles, float relativeError = 0.01f, Option<string> where = default)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName),
                column.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$1")),
                quantiles,
                relativeError,
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$4")));
        }
    }

    /// <summary>
    /// Completeness computes the fraction of non-null values ina column of a <see cref="Microsoft.Spark.Sql.DataFrame"/>
    /// </summary>
    public class Completeness : AnalyzerJvmBase
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="Completeness"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Functions.Expr"/>.</param>
        public Completeness(string column, Option<string> where = default) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Compliance is a measure of the fraction of rows that complies with the given column constraint.
    /// E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value greater than
    /// 3 and 10 rows under 3; a <see cref="DoubleMetric"/> would be returned with 0.33 value
    /// </summary>
    public class Compliance : AnalyzerJvmBase
    {
        /// <summary>
        /// Describe the compliance.
        /// </summary>
       // public string Instance;
        /// <summary>
        /// SQL-Like predicate to apply per row <see cref="Functions.Expr"/>.
        /// </summary>
     //   public readonly Option<string> Predicate;
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
        public Compliance(string instance, Option<string> predicate, Option<string> where = default)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName), instance,
                predicate.Value,
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$3")));
        }
    }

    /// <summary>
    /// Computes the pearson correlation coefficient between the two given columns
    /// </summary>
    public class Correlation : AnalyzerJvmBase
    {
        /// <summary>
        /// First input column for computation.
        /// </summary>
      //  public readonly string ColumnA;
        /// <summary>
        /// Second input column for computation.
        /// </summary>
    //    public readonly string ColumnB;

        /// <summary>
        /// Initializes a new instance of the <see cref="Correlation"/> class.
        /// </summary>
        /// <param name="columnA">First input column for computation</param>
        /// <param name="columnB">Second input column for computation.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public Correlation(string columnA, string columnB, Option<string> where = default)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName),
                columnA, columnB, where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$3")));
        }
    }


    /// <summary>
    /// Counts the distinct elements in the column(s).
    /// </summary>
    public class CountDistinct : AnalyzerJvmBase
    {
        /// <summary>
        /// Columns to search on.
        /// </summary>
        //private readonly IEnumerable<string> Columns;

        /// <summary>
        /// Initializes a new instance of the <see cref="CountDistinct"/> class.
        /// </summary>
        /// <param name="columns">Columns to search on.</param>
        public CountDistinct(IEnumerable<string> columns)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(AnalyzersNamespaces(AnalyzerName), columns);
        }
    }


    /// <summary>
    /// Data type analyzers, analyzes the data type of the target column.
    /// </summary>
    public class DataType : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DataType"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">A string representing the where clause to include <see cref="Functions.Expr"/>.</param>
        public DataType(string column, Option<string> where) : base(column, where)
        {
        }
    }

    public enum DataTypeInstances
    {
        Unknown = 0,
        Fractional = 1,
        Integral = 2,
        Boolean = 3,
        String = 4
    }

    /// <summary>
    /// Distinctness is the fraction of distinct values of a column(s).
    /// </summary>
    public class Distinctness : AnalyzerJvmBase
    {
        // private IEnumerable<string> Columns;

        /// <summary>
        /// Initializes a new instance of type <see cref="Distinctness"/> class.
        /// </summary>
        /// <param name="columns">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public Distinctness(IEnumerable<string> columns, Option<string> where) : base(where)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName), new SeqJvm(columns.ToArray()).Reference,
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$2")));
        }
    }


    /// <summary>
    /// Entropy is a measure of the level of information contained in a message. Given the probability
    /// distribution over values in a column, it describes how many bits are required to identify a value.
    /// </summary>
    public class Entropy : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Entropy"/> class.
        /// </summary>
        /// <param name="column">The target column names subject to the grouping.</param>
        /// <param name="where">A where clause to filter only some values in a column <see cref="Expr"/>.</param>
        public Entropy(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Computes the max value for the target column.
    /// </summary>
    public class Maximum : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Maximum"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public Maximum(string column, Option<string> where = default) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Computes the max value for the target column.
    /// </summary>
    public class MaxLength : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="MaxLength"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MaxLength(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Computes the mean for the target column.
    /// </summary>
    public class Mean : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Mean"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public Mean(string column, Option<string> where) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Computes the min value for the target column.
    /// </summary>
    public class Minimum : AnalyzerJvmBase, IAnalyzer<IMetric>
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Minimum"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public Minimum(string column, Option<string> where) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Computes the min value for the target column.
    /// </summary>
    public class MinLength : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="MinLength"/>.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MinLength(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Mutual Information describes how much information about one column can be inferred from another
    /// column. If two columns are independent of each other, then nothing can be inferred from one column about
    /// the other, and mutual information is zero. If there is a functional dependency of one column to
    /// another and vice versa, then all information of the two columns are shared, and mutual information is the entropy of each column.
    /// </summary>
    public class MutualInformation : AnalyzerJvmBase
    {
        // private IEnumerable<string> Columns;
        /// <summary>
        /// Initializes a new instance of type <see cref="MutualInformation"/> class.
        /// </summary>
        /// <param name="columns">The target column names.</param>
        /// <param name="where">The where condition target of the invocation</param>
        public MutualInformation(IEnumerable<string> columns, Option<string> where = default) : base(where)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName),
                new SeqJvm(columns.ToArray()).Reference, where.ToJvm((AnalyzersNamespaces(AnalyzerName),"apply$default$2")));
        }
    }



    /// <summary>
    /// PatternMatch is a measure of the fraction of rows that complies with a given column regex constraint.
    /// E.g if the constraint is Patterns.CREDITCARD and the data frame has 5 rows which contain a credit card number in a certain column
    /// according to the regex and and 10 rows that do not, a DoubleMetric would be
    /// returned with 0.33 as value
    /// </summary>
    public class PatternMatch : AnalyzerJvmBase
    {
        /// <summary>
        /// Column to do the pattern match analysis on.
        /// </summary>
      //  public readonly JvmObjectReference Regex;

        /// <summary>
        /// Initializes a new instance of type <see cref="PatternMatch"/> class.
        /// </summary>
        /// <param name="column">Column to do the pattern match analysis on.</param>
        /// <param name="regex">The regular expression to check for.</param>
        /// <param name="where">Additional filter to apply before the analyzer is run.</param>
        public PatternMatch(string column, string regex, Option<string> where)
        {
             _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                 AnalyzersNamespaces(AnalyzerName),
                 column,
                 SparkEnvironment.JvmBridge.CallConstructor("scala.util.matching.Regex", regex, null),
                 where.ToJvm((AnalyzersNamespaces(AnalyzerName),"apply$default$3")));
        }
    }


    public static class Patterns
    {
        // scalastyle:off
        // http://emailregex.com
        public static Regex Email => new Regex(
            @"(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\""(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\\"")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])");

        // https://mathiasbynens.be/demo/url-regex stephenhay
        public static Regex Url => new Regex(@"(https?|ftp)://[^\s/$.?#].[^\s]*");

        public static Regex SocialSecurityNumberUs => new Regex(
            @"((?!219-09-9999|078-05-1120)(?!666|000|9\d{2})\d{3}-(?!00)\d{2}-(?!0{4})\d{4})|((?!219 09 9999|078 05 1120)(?!666|000|9\d{2})\d{3} (?!00)\d{2} (?!0{4})\d{4})|((?!219099999|078051120)(?!666|000|9\d{2})\d{3}(?!00)\d{2}(?!0{4})\d{4})");

        public static Regex CreditCard =>
            new Regex(
                @"\b(?:3[47]\d{2}([\ \-]?)\d{6}\1\d|(?:(?:4\d|5[1-5]|65)\d{2}|6011)([\ \-]?)\d{4}\2\d{4}\2)\d{4}\b");
    }


    /// <summary>
    /// Size computes the number of rows in a <see cref="DataFrame"/>.
    /// </summary>
    public class Size : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Size"/> class.
        /// </summary>
        /// <param name="where">Additional filter to apply before the analyzer is run.</param>
        public Size(Option<string> where)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName),
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$1")));
        }
    }

    /// <summary>
    /// Computes the standard deviation of a column.
    /// </summary>
    public class StandardDeviation : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="StandardDeviation"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public StandardDeviation(string column, Option<string> where) : base(column, where)
        {
        }
    }

    /// <summary>
    /// Computes the sum of data.
    /// </summary>
    public class Sum : AnalyzerJvmBase
    {
        /// <summary>
        /// Initializes a new instance of type <see cref="Sum"/> class.
        /// </summary>
        /// <param name="column">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public Sum(string column, Option<string> where) : base(column, where)
        {
        }
    }


    /// <summary>
    /// Uniqueness is the fraction of unique values of a column(s), i.e., values that occur exactly once.
    /// </summary>
    public class Uniqueness : AnalyzerJvmBase
    {
      //  private IEnumerable<string> Columns;
        /// <summary>
        /// Initializes a new instance of type <see cref="Uniqueness"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public Uniqueness(IEnumerable<string> columns, Option<string> where = default)
        {
           _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName),
                new SeqJvm( columns.ToArray()).Reference,
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$2")));
        }
    }

    /// <summary>
    /// Computes the unique value ratio of a <see cref="DataFrame"/> column.
    /// </summary>
    public class UniqueValueRatio : AnalyzerJvmBase
    {
        private IEnumerable<string> Columns;
        /// <summary>
        /// Initializes a new instance of type <see cref="Uniqueness"/> class.
        /// </summary>
        /// <param name="columns">The target column name.</param>
        /// <param name="where">The where condition target of the invocation.</param>
        public UniqueValueRatio(IEnumerable<string> columns, Option<string> where)
        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(
                AnalyzersNamespaces(AnalyzerName),
                new SeqJvm(columns.ToArray()).Reference,
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$2")));
        }
    }


    public class Histogram : AnalyzerJvmBase
    {
        public static string NULL_FIELD_REPLACEMENT = "NullValue";

        public Histogram(string column,
            Option<UserDefinedFunction> binningUdf,
            Option<int> maxDetailsBin = default,
            Option<string> where = default)

        {
            _jvmBridge = SparkEnvironment.JvmBridge.CallConstructor(AnalyzersNamespaces(AnalyzerName),
                column,
                binningUdf.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$2")),
                maxDetailsBin.GetOrElse(1000),
                where.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$4"))
            );
        }

    }

    /// <summary>
    ///  Parameter definition for KLL Sketches.
    /// </summary>
    public class KLLParameters
    {
        private readonly int _sketchSize;
        private readonly float _shrinkingFactor;
        private readonly int _numberOfBuckets;

        /// <summary>
        ///
        /// </summary>
        /// <param name="sketchSize"></param>
        /// <param name="shrinkingFactor"></param>
        /// <param name="numberOfBuckets"></param>
        public KLLParameters(int sketchSize, float shrinkingFactor, int numberOfBuckets)
        {
            _sketchSize = sketchSize;
            _shrinkingFactor = shrinkingFactor;
            _numberOfBuckets = numberOfBuckets;
        }

        /// <summary>
        /// Return the JVM KLLParameter object
        /// </summary>
        /// <returns></returns>
        public JvmObjectReference ToJvm()
        {
            return SparkEnvironment.JvmBridge.CallConstructor("com.amazon.deequ.analyzers.KLLParameters",
                _sketchSize,
                _shrinkingFactor,
                _numberOfBuckets);
        }
    }

    public class KLLSketch : AnalyzerJvmBase
    {

        public KLLSketch(string column, Option<KLLParameters> kllParameters) : base(column)
        {
            _jvmBridge = SparkEnvironment.JvmBridge
                .CallConstructor(AnalyzersNamespaces(AnalyzerName),
                    column,
                    kllParameters.ToJvm((AnalyzersNamespaces(AnalyzerName), "apply$default$2"))
                );
        }
    }
}
