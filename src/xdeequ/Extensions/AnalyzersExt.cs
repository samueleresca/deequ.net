using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using xdeequ.Analyzers;
using xdeequ.Analyzers.Runners;
using xdeequ.Analyzers.States;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;
using DataType = Microsoft.Spark.Sql.Types.DataType;

namespace xdeequ.Extensions
{
    public static class AnalyzersExt
    {
        public static string COL_PREFIX = "xdeequ_dq_metrics_";
        public static string COUNT_COL = $"${COL_PREFIX}count";

        public static List<string> NumericDataType = new List<string>
        {
            "ByteType",
            "ShortType",
            "IntegerType",
            "LongType",
            "FloatType",
            "DoubleType",
            "DecimalType"
        };

        public static Option<S> Merge<S>(Option<S> state, Option<S> anotherState) where S : State<S>
        {
            IEnumerable<Option<S>> statesToMerge = new List<Option<S>> {state, anotherState};

            return statesToMerge.Aggregate((stateA, stateB) =>
            {
                S state = (stateA.HasValue, stateB.HasValue) switch
                {
                    (true, true) => stateA.Value.Sum(stateB.Value),
                    (true, false) => stateA.Value,
                    (false, true) => stateB.Value,
                    _ => null
                };

                return state == null ? Option<S>.None : new Option<S>(state);
            });
        }

        public static DoubleMetric MetricFromValue(Try<double> value, string name, string instance,
            Entity entity = Entity.Column) =>
            DoubleMetric.Create(entity, name, instance, value.Get());

        public static Column ConditionalSelection(Column selection, Option<string> condition)
        {
            Option<Column> conditionColumn = condition.Select(cond => Expr(cond));
            return ConditionalSelectionFromColumns(selection, conditionColumn);
        }

        public static bool HasColumn(StructType schema, string column) => schema.Fields.Select(x => x.Name)
            .Contains(column, StringComparer.InvariantCultureIgnoreCase);

        public static Action<StructType> HasColumn(string column) =>
            schema =>
            {
                if (!HasColumn(schema, column))
                {
                    throw new Exception($"Input data does not include column {column}!");
                }
            };

        public static Action<StructType> IsNumeric(string column) =>
            schema =>
            {
                DataType columnDataType = StructField(column, schema).DataType;

                bool hasNumericType = columnDataType.TypeName == new ByteType().TypeName ||
                                      columnDataType.TypeName == new ShortType().TypeName ||
                                      columnDataType.TypeName == new IntegerType().TypeName ||
                                      columnDataType.TypeName == new LongType().TypeName ||
                                      columnDataType.TypeName == new FloatType().TypeName ||
                                      columnDataType.TypeName == new DecimalType().TypeName;

                if (!hasNumericType)
                {
                    throw new Exception(
                        $"Expected type of column $column to be one of ${string.Join(',', NumericDataType)}), but found ${columnDataType} instead!");
                }
            };

        public static Action<StructType> IsString(string column) =>
            schema =>
            {
                DataType columnDataType = StructField(column, schema).DataType;

                bool hasNumericType = columnDataType.TypeName == new StringType().TypeName;

                if (!hasNumericType)
                {
                    throw new Exception(
                        $"Expected type of column $column to be StringType, but found ${columnDataType} instead!");
                }
            };

        public static Option<S> IfNoNullsIn<S>(Row result, int offset, Func<S> func, int howMany = 1)
        {
            for (int i = offset; i < offset + howMany; i++)
            {
                if (result[i] == null)
                {
                    return Option<S>.None;
                }
            }

            return new Option<S>(func());
        }

        private static Column ConditionalSelectionFromColumns(Column selection, Option<Column> conditionColumn) =>
            conditionColumn
                .Select(condition => When(condition, selection))
                .GetOrElse(selection);

        public static Action<StructType> IsNotNested(string column) =>
            schema =>
            {
                if (!HasColumn(schema, column))
                {
                    return;
                }

                DataType columnDataType = StructField(column, schema).DataType;
                if (
                    columnDataType.TypeName == "StructType" ||
                    columnDataType.TypeName == "MapType" ||
                    columnDataType.TypeName == "ArrayType"
                )
                {
                    throw new Exception($"Unsupported nested column type of column {column}: {columnDataType}!");
                }
            };

        public static StructField StructField(string column, StructType schema)
        {
            StructField structFields = schema.Fields.First(x => x.Name == column);
            return structFields;
        }

        public static DoubleMetric MetricFromFailure(Exception exception, string name, string instance,
            Entity entity = Entity.Column) =>
            DoubleMetric.Create(entity, name, instance,
                new Try<double>(ExceptionExt.WrapIfNecessary(exception)));

        public static DoubleMetric MetricFromEmpty<S, T>(Analyzer<S, T> analyzer, string name, string instance,
            Entity entity = Entity.Column) where S : State<S>, IState
        {
            EmptyStateException emptyState =
                new EmptyStateException($"Empty state for analyzer {analyzer}, all input values were NULL.");
            return DoubleMetric.Create(entity, name, instance,
                new Try<double>(ExceptionExt.WrapIfNecessary(emptyState)));
        }

        public static Column ConditionalCount(Option<string> where) =>
            where
                .Select(filter => Sum(Expr(where.Value).Cast("long")))
                .GetOrElse(Count("*"));

        public static void Merge<K, V>(this IDictionary<K, V> target, IDictionary<K, V> source,
            bool overwrite = false) =>
            source.ToList().ForEach(_ =>
            {
                if (!target.ContainsKey(_.Key) || overwrite)
                {
                    target[_.Key] = _.Value;
                }
            });

        public static Column ConditionalSelection(Option<string> column, Option<string> where) =>
            ConditionalSelection(Column(column.Value), where);

        public static Column ZeroIfNull(string column) => Coalesce(Col(column), Lit(0));

        public static Action<StructType> AtLeastOne(IEnumerable<string> columns) =>
            type =>
            {
                if (columns.Any(x => x == string.Empty))
                {
                    throw new Exception("At least one column needs to be specified!");
                }
            };

        public static IEnumerable<Action<StructType>> ExactlyNColumns(IEnumerable<string> columns, int n) =>
            new[]
            {
                (Action<StructType>)(type =>
                {
                    if (columns.Count() != n)
                    {
                        throw new Exception(
                            $"In columns have to be specified! Currently, columns contains only {columns.Count()} column(s): ${string.Join(',', columns)}!");
                    }
                })
            }.AsEnumerable();

        public static Entity EntityFrom(IEnumerable<string> enumerable) =>
            enumerable.Count() == 1 ? Entity.Column : Entity.MultiColumn;
    }
}
