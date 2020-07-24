using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Checks;
using xdeequ.Constraints;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;

namespace xdeequ.Analyzers.Applicability
{
    public class ApplicabilityResult
    {
        public Dictionary<string, Option<Exception>> Features;
        public bool IsApplicable;
    }

    public class CheckApplicability : ApplicabilityResult
    {
        public Dictionary<IConstraint, bool> ConstraintApplicabilities;

        public CheckApplicability(bool isApplicable, Dictionary<string, Option<Exception>> features,
            Dictionary<IConstraint, bool> constraintApplicabilities)
        {
            IsApplicable = isApplicable;
            Features = features;
            ConstraintApplicabilities = constraintApplicabilities;
        }
    }

    public class AnalyzersApplicability : ApplicabilityResult
    {
        public AnalyzersApplicability(bool isApplicable, Dictionary<string, Option<Exception>> features)
        {
            IsApplicable = isApplicable;
            Features = features;
        }
    }

    public class Applicability
    {
        private readonly SparkSession _session;

        public Applicability(SparkSession session) => _session = session;


        public CheckApplicability IsApplicable(Check check, StructType schema)
        {
            DataFrame data = GenerateRandomData(schema, 1000);

            IEnumerable<KeyValuePair<string, IMetric>> namedMetrics = check.Constraints
                .Select(x => new KeyValuePair<string, IConstraint>(x.ToString(), x))
                .Select(x =>
                {
                    if (x.Value is ConstraintDecorator cd)
                    {
                        return new KeyValuePair<string, IConstraint>(x.Key, cd.Inner);
                    }

                    return x;
                })
                .Select(x =>
                {
                    IAnalysisBasedConstraint analyzer = (IAnalysisBasedConstraint)x.Value;
                    IMetric metric = analyzer.Analyzer.Calculate(data);

                    return new KeyValuePair<string, IMetric>(x.Key, metric);
                });

            IEnumerable<KeyValuePair<IConstraint, bool>> constraintApplicabilities = check.Constraints
                .Zip(namedMetrics,
                    (constraint, pair) => new KeyValuePair<IConstraint, bool>(constraint, pair.Value.IsSuccess()));

            IEnumerable<KeyValuePair<string, Option<Exception>>> failures = namedMetrics
                .Select(pair =>
                    pair.Value.Exception().HasValue
                        ? new KeyValuePair<string, Option<Exception>>(pair.Key, pair.Value.Exception())
                        : new KeyValuePair<string, Option<Exception>>(pair.Key, Option<Exception>.None));


            return new CheckApplicability(!failures.Any(),
                new Dictionary<string, Option<Exception>>(failures),
                new Dictionary<IConstraint, bool>(constraintApplicabilities));
        }

        public AnalyzersApplicability IsApplicable(IEnumerable<IAnalyzer<IMetric>> analyzers, StructType schema)
        {
            DataFrame data = GenerateRandomData(schema, 1000);

            IEnumerable<KeyValuePair<string, IAnalyzer<IMetric>>> analyzersByName = analyzers
                .Select(x => new KeyValuePair<string, IAnalyzer<IMetric>>(x.ToString(), x));

            IEnumerable<KeyValuePair<string, Option<Exception>>> failures = analyzersByName
                .Select(pair =>
                {
                    IMetric maybeValue = pair.Value.Calculate(data);
                    return new KeyValuePair<string, Option<Exception>>(pair.Key, maybeValue.Exception());
                });

            return new AnalyzersApplicability(!failures.Any(),
                new Dictionary<string, Option<Exception>>(failures));
        }

        private DataFrame GenerateRandomData(StructType schema, int numRows)
        {
            IEnumerable<GenericRow> rows = Enumerable.Range(0, numRows).Select(field =>
            {
                IEnumerable<object> cells = schema.Fields.Select<StructField, object>(x =>
                {
                    if (x is StringType)
                    {
                        return RandomString(x.IsNullable);
                    }

                    if (x is IntegerType)
                    {
                        return RandomInteger(x.IsNullable);
                    }

                    if (x is FloatType)
                    {
                        return RandomFloat(x.IsNullable);
                    }

                    if (x is DoubleType)
                    {
                        return RandomDouble(x.IsNullable);
                    }

                    if (x is ByteType)
                    {
                        return RandomByte(x.IsNullable);
                    }

                    if (x is ShortType)
                    {
                        return RandomShort(x.IsNullable);
                    }

                    if (x is LongType)
                    {
                        return RandomLong(x.IsNullable);
                    }

                    if (x is DecimalType)
                    {
                        return RandomDecimal(x.IsNullable);
                    }

                    if (x is TimestampType)
                    {
                        return RandomTimestamp(x.IsNullable);
                    }

                    if (x is BooleanType)
                    {
                        return RandomBoolean(x.IsNullable);
                    }

                    throw new ArgumentException(
                        $"Applicability check can only handle basic datatypes for columns (string, integer, float, double, decimal, boolean) not {x.DataType}");
                });
                return new GenericRow(cells.ToArray());
            });

            return _session.CreateDataFrame(rows, schema);
        }

        private static bool ShouldBeNull(bool nullable)
        {
            Random random = new Random();
            return nullable && random.NextDouble() < 0.01;
        }

        private static bool? RandomBoolean(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return new Random().NextDouble() > 0.5;
        }

        private static object RandomInteger(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return new Random().Next();
        }

        private static object RandomFloat(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return new Random().Next();
        }

        private static object RandomDouble(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return new Random().NextDouble();
        }

        private static object RandomByte(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return (byte)new Random().Next();
        }

        private static object RandomShort(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return (short)new Random().Next();
        }

        private static object RandomLong(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return new Random().Next();
        }

        private static object RandomDecimal(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return new Random().NextDecimal();
        }

        private static object RandomTimestamp(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            return new Timestamp(new DateTime(new Random().Next()));
        }

        private static object RandomString(bool nullable)
        {
            if (ShouldBeNull(nullable))
            {
                return null;
            }

            int length = new Random().Next(20) + 1;
            return new Random().RandomString(length);
        }
    }
}
