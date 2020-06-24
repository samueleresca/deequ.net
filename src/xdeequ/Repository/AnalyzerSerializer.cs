using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using Microsoft.Spark.Sql;
using xdeequ.Analyzers;
using xdeequ.Extensions;
using xdeequ.Metrics;
using xdeequ.Util;
using static Microsoft.Spark.Sql.Functions;

namespace xdeequ.Repository
{
    public class AnalyzerSerializer : JsonConverter<IAnalyzer<IMetric>>
    {
        public override IAnalyzer<IMetric> Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            JsonDocument.TryParseValue(ref reader, out JsonDocument document);

            string analyzerType = document.RootElement.GetProperty(SerdeExt.ANALYZER_NAME_FIELD).GetString();

            if (analyzerType == "Size")
            {
                return new Size(GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "Completeness")
            {
                return new Completeness(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "Compliance")
            {
                return new Compliance(
                    document.RootElement.GetProperty("instance").GetString(),
                    Column(document.RootElement.GetProperty("predicate").GetString()),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "PatternMatch")
            {
                return new PatternMatch(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    new Regex(document.RootElement.GetProperty("instance").GetString()),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "Sum")
            {
                return new Sum(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "Mean")
            {
                return new Mean(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "Minimum")
            {
                return new Minimum(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "Maximum")
            {
                return new Maximum(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "CountDistinct")
            {
                throw new NotImplementedException();
            }

            if (analyzerType == "Distinctness")
            {
                return new Distinctness(GetColumnAsSequence(document.RootElement.GetProperty(SerdeExt.COLUMNS_FIELD)));
            }

            if (analyzerType == "Entropy")
            {
                return new Entropy(document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString());
            }

            if (analyzerType == "MutualInformation")
            {
                return new MutualInformation(
                    GetColumnAsSequence(document.RootElement.GetProperty(SerdeExt.COLUMNS_FIELD)));
            }

            if (analyzerType == "UniqueValueRatio")
            {
                return new UniqueValueRatio(
                    GetColumnAsSequence(document.RootElement.GetProperty(SerdeExt.COLUMNS_FIELD)));
            }

            if (analyzerType == "Uniqueness")
            {
                return new Uniqueness(GetColumnAsSequence(document.RootElement.GetProperty(SerdeExt.COLUMNS_FIELD)));
            }

            if (analyzerType == "Histogram")
            {
                return new Histogram(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    Option<string>.None,
                    Option<Func<Column, Column>>.None,
                    document.RootElement.GetProperty("maxDetailBins").GetInt32());
            }

            if (analyzerType == "ApproxCountDistinct")
            {
                throw new NotImplementedException();
            }

            if (analyzerType == "Correlation")
            {
                throw new NotImplementedException();
            }

            if (analyzerType == "DataType")
            {
                return new DataType(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "ApproxQuantile")
            {
                throw new NotImplementedException();
            }

            if (analyzerType == "ApproxQuantiles")
            {
                throw new NotImplementedException();
            }

            if (analyzerType == "StandardDeviation")
            {
                return new StandardDeviation(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "MinLength")
            {
                return new MinLength(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            if (analyzerType == "MaxLength")
            {
                return new MaxLength(
                    document.RootElement.GetProperty(SerdeExt.COLUMN_FIELD).GetString(),
                    GetOptionalWhereParam(document.RootElement));
            }

            throw new ArgumentException($"Unable to deserialize analyzer {analyzerType}");
        }

        public override void Write(Utf8JsonWriter writer, IAnalyzer<IMetric> analyzer, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            if (analyzer is Size size)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Size");
                writer.WriteString(SerdeExt.WHERE_FIELD, size.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is Completeness completeness)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Completeness");
                writer.WriteString(SerdeExt.COLUMN_FIELD, completeness.Column.GetOrElse(string.Empty));
                writer.WriteString(SerdeExt.WHERE_FIELD, completeness.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is Compliance compliance)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Compliance");
                writer.WriteString(SerdeExt.WHERE_FIELD, compliance.Where.GetOrElse(string.Empty));
                writer.WriteString("instance", compliance.Instance);
                writer.WriteString("predicate", compliance.Predicate.ToString());
                writer.WriteEndObject();
                return;
            }

            if (analyzer is PatternMatch patternMatch)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "PatternMatch");
                writer.WriteString(SerdeExt.COLUMN_FIELD, patternMatch.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, patternMatch.Where.GetOrElse(string.Empty));
                writer.WriteString("pattern", patternMatch.Regex.ToString());
                writer.WriteEndObject();
                return;
            }

            if (analyzer is Sum sum)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Sum");
                writer.WriteString(SerdeExt.COLUMN_FIELD, sum.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, sum.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is Mean mean)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Mean");
                writer.WriteString(SerdeExt.COLUMN_FIELD, mean.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, mean.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is Minimum min)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Minimum");
                writer.WriteString(SerdeExt.COLUMN_FIELD, min.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, min.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }


            if (analyzer is Maximum max)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Maximum");
                writer.WriteString(SerdeExt.COLUMN_FIELD, max.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, max.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }


            //TODO: missing if (analyzer is CountDistinct max)

            if (analyzer is Distinctness distinctness)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Distinctness");
                writer.WriteArray(SerdeExt.COLUMN_FIELD, distinctness.Columns.Select(x => x.ToString()));
                writer.WriteString(SerdeExt.WHERE_FIELD, distinctness.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is Entropy entropy)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Entropy");
                writer.WriteString(SerdeExt.COLUMN_FIELD, entropy.Column.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is MutualInformation mutual)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "MutualInformation");
                writer.WriteArray(SerdeExt.COLUMN_FIELD, mutual.Columns.Select(x => x.ToString()));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is UniqueValueRatio uniqueValueRatio)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "UniqueValueRatio");
                writer.WriteArray(SerdeExt.COLUMN_FIELD, uniqueValueRatio.Columns.Select(x => x.ToString()));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is Uniqueness uniqueness)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Uniqueness");
                writer.WriteArray(SerdeExt.COLUMN_FIELD, uniqueness.Columns.Select(x => x.ToString()));
                writer.WriteEndObject();
                return;
            }


            if (analyzer is Histogram histogram)
            {
                if (histogram.BinningUdf.HasValue)
                {
                    throw new ArgumentException("Unable to serialize Histogram with binningUdf!");
                }

                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "Histogram");
                writer.WriteString(SerdeExt.COLUMN_FIELD, histogram.Column);
                writer.WriteNumber("maxDetailBins", histogram.maxDetailBins);
                writer.WriteEndObject();
                return;
            }

            if (analyzer is DataType dataType)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "DataType");
                writer.WriteString(SerdeExt.COLUMN_FIELD, dataType.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, dataType.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is ApproxCountDistinct approxCountDistinct)
            {
                throw new NotImplementedException();
            }

            if (analyzer is Correlation correlation)
            {
                throw new NotImplementedException();
            }

            if (analyzer is StandardDeviation stDev)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "StandardDeviation");
                writer.WriteString(SerdeExt.COLUMN_FIELD, stDev.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, stDev.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is ApproxQuantile approxQuantile)
            {
                throw new NotImplementedException();
            }

            if (analyzer is MinLength minLength)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "MinLength");
                writer.WriteString(SerdeExt.COLUMN_FIELD, minLength.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, minLength.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            if (analyzer is MaxLength maxLength)
            {
                writer.WriteString(SerdeExt.ANALYZER_NAME_FIELD, "MaxLength");
                writer.WriteString(SerdeExt.COLUMN_FIELD, maxLength.Column);
                writer.WriteString(SerdeExt.WHERE_FIELD, maxLength.Where.GetOrElse(string.Empty));
                writer.WriteEndObject();
                return;
            }

            throw new ArgumentException($"Unable to serialize analyzer {analyzer}");
        }

        private Option<string> GetOptionalWhereParam(JsonElement jsonElement)
        {
            if (jsonElement.TryGetProperty(SerdeExt.WHERE_FIELD, out JsonElement element)
                && element.GetString() != string.Empty)
            {

                return new Option<string>(element.GetString());
            }

            return Option<string>.None;
        }

        private IEnumerable<string> GetColumnAsSequence(JsonElement jsonElement) => jsonElement.EnumerateArray().Select(x => x.GetString());
    }
}
