using System;
using System.Collections.Generic;
using System.Linq;
using deequ.Checks;
using deequ.Constraints;
using deequ.Extensions;
using deequ.Metrics;
using deequ.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace deequ.Analyzers.Applicability
{
    class ApplicabilityResult
    {
        public Dictionary<string, Option<Exception>> Features;
        public bool IsApplicable;
    }

    class CheckApplicability : ApplicabilityResult
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

    class AnalyzersApplicability : ApplicabilityResult
    {
        public AnalyzersApplicability(bool isApplicable, Dictionary<string, Option<Exception>> features)
        {
            IsApplicable = isApplicable;
            Features = features;
        }
    }

    class Applicability
    {
        private readonly SparkSession _session;

        public Applicability(SparkSession session) => _session = session;


        public CheckApplicability IsApplicable(Check check, StructType schema)
        {
            DataFrame data = GenerateRandomData(schema, 1000);

            IEnumerable<KeyValuePair<string, IMetric>> namedMetrics = check.Constraints
                .Select(constraint => new KeyValuePair<string, IConstraint>(constraint.ToString(), constraint))
                .Select(keyValuePair =>
                {
                    if (keyValuePair.Value is ConstraintDecorator cd)
                    {
                        return new KeyValuePair<string, IConstraint>(keyValuePair.Key, cd.Inner);
                    }

                    return keyValuePair;
                })
                .Select(keyValuePair =>
                {
                    IAnalysisBasedConstraint analyzer = (IAnalysisBasedConstraint)keyValuePair.Value;
                    IMetric metric = analyzer.Analyzer.Calculate(data);

                    return new KeyValuePair<string, IMetric>(keyValuePair.Key, metric);
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
                .Select(analyzer => new KeyValuePair<string, IAnalyzer<IMetric>>(analyzer.ToString(), analyzer));

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
                IEnumerable<object> cells = schema.Fields.Select(field =>
                {
                    if (field is StringType)
                    {
                        return RandomString(field.IsNullable);
                    }

                    if (field is IntegerType)
                    {
                        return RandomInteger(field.IsNullable);
                    }

                    if (field is FloatType)
                    {
                        return RandomFloat(field.IsNullable);
                    }

                    if (field is DoubleType)
                    {
                        return RandomDouble(field.IsNullable);
                    }

                    if (field is ByteType)
                    {
                        return RandomByte(field.IsNullable);
                    }

                    if (field is ShortType)
                    {
                        return RandomShort(field.IsNullable);
                    }

                    if (field is LongType)
                    {
                        return RandomLong(field.IsNullable);
                    }

                    if (field is DecimalType)
                    {
                        return RandomDecimal(field.IsNullable);
                    }

                    if (field is TimestampType)
                    {
                        return RandomTimestamp(field.IsNullable);
                    }

                    if (field is BooleanType)
                    {
                        return RandomBoolean(field.IsNullable);
                    }

                    throw new ArgumentException(
                        $"Applicability check can only handle basic datatypes for columns (string, integer, float, double, decimal, boolean) not {field.DataType}");
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
