using System;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;

namespace DataStage2Airflow.Dsx
{
    /// <summary>
    /// Parser for the DataStage XML export format:
    /// <code>
    /// &lt;DSExport&gt;
    ///   &lt;Header ToolInstanceID="PROJ" ServerVersion="7.5" .../&gt;
    ///   &lt;Job Identifier="LoadCustomers" DateModified="..."&gt;
    ///     &lt;Record Identifier="ROOT" Type="JobDefn" Readonly="0"&gt;
    ///       &lt;Property Name="Name"&gt;LoadCustomers&lt;/Property&gt;
    ///       &lt;Collection Name="Parameters" Type="Parameters"&gt;
    ///         &lt;SubRecord&gt;&lt;Property Name="Name"&gt;SourceDir&lt;/Property&gt;&lt;/SubRecord&gt;
    ///       &lt;/Collection&gt;
    ///     &lt;/Record&gt;
    ///   &lt;/Job&gt;
    /// &lt;/DSExport&gt;
    /// </code>
    /// It produces the same <see cref="DsxDocument"/> as the DSX reader. Record types come without the
    /// leading "C" of the DSX OLE type (JobDefn vs CJobDefn) and are normalised here.
    /// </summary>
    public static class XmlExportReader
    {
        public static DsxDocument Parse(TextReader text, string sourceName, DiagnosticBag? diagnostics = null)
        {
            diagnostics = diagnostics ?? new DiagnosticBag();
            var settings = new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Ignore,
                XmlResolver = null,
                CheckCharacters = false,
                IgnoreComments = true,
            };

            XDocument xml;
            try
            {
                using (var reader = XmlReader.Create(text, settings))
                {
                    xml = XDocument.Load(reader, LoadOptions.SetLineInfo);
                }
            }
            catch (XmlException ex)
            {
                throw new DsxFormatException($"{sourceName}: invalid XML export: {ex.Message}");
            }

            var root = xml.Root;
            if (root == null || !string.Equals(root.Name.LocalName, "DSExport", StringComparison.OrdinalIgnoreCase))
            {
                throw new DsxFormatException($"{sourceName}: not a DataStage XML export (root element must be DSExport)");
            }

            var document = new DsxDocument(sourceName, ExportFormat.Xml);
            foreach (var element in root.Elements())
            {
                if (string.Equals(element.Name.LocalName, "Header", StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var attribute in element.Attributes()) document.Header.Add(attribute.Name.LocalName, attribute.Value);
                    continue;
                }

                document.Objects.Add(ReadObject(element, sourceName, diagnostics));
            }

            return document;
        }

        public static string NormalizeOleType(string type)
        {
            if (type.Length == 0) return type;
            bool hasPrefix = type.Length > 1 && type[0] == 'C' && char.IsUpper(type[1]);
            return hasPrefix ? type : "C" + type;
        }

        private static DsxObject ReadObject(XElement element, string sourceName, DiagnosticBag diagnostics)
        {
            var obj = new DsxObject(SectionFor(element.Name.LocalName)) { Line = LineOf(element) };
            foreach (var attribute in element.Attributes())
            {
                if (attribute.Name.LocalName == "Identifier")
                {
                    obj.Identifier = attribute.Value;
                }
                else
                {
                    obj.Properties.Add(attribute.Name.LocalName, attribute.Value);
                }
            }

            foreach (var child in element.Elements())
            {
                switch (child.Name.LocalName)
                {
                    case "Record":
                        obj.Records.Add(ReadRecord(child));
                        break;
                    case "Property":
                        obj.Properties.Add(NameOf(child), ValueOf(child));
                        break;
                    default:
                        if (child.HasElements)
                        {
                            obj.Children.Add(ReadObject(child, sourceName, diagnostics));
                        }
                        else
                        {
                            diagnostics.Info("XML001", $"ignored element <{child.Name.LocalName}>", $"{sourceName}:{LineOf(child)}");
                        }

                        break;
                }
            }

            return obj;
        }

        private static DsxRecord ReadRecord(XElement element)
        {
            var record = new DsxRecord((string?)element.Attribute("Identifier") ?? string.Empty) { Line = LineOf(element) };
            foreach (var attribute in element.Attributes())
            {
                switch (attribute.Name.LocalName)
                {
                    case "Identifier":
                        break;
                    case "Type":
                        record.Properties.Add("OLEType", NormalizeOleType(attribute.Value));
                        break;
                    default:
                        record.Properties.Add(attribute.Name.LocalName, attribute.Value);
                        break;
                }
            }

            foreach (var child in element.Elements())
            {
                if (child.Name.LocalName == "Property")
                {
                    record.Properties.Add(NameOf(child), ValueOf(child));
                }
                else if (child.Name.LocalName == "Collection")
                {
                    var name = (string?)child.Attribute("Name") ?? string.Empty;
                    var type = NormalizeOleType((string?)child.Attribute("Type") ?? string.Empty);
                    record.Properties.Add(name, type);
                    var collection = record.Collection(name);
                    if (collection == null)
                    {
                        collection = new DsxCollection(name, type);
                        record.Collections.Add(collection);
                    }

                    foreach (var sub in child.Elements().Where(e => e.Name.LocalName == "SubRecord"))
                    {
                        var bag = new DsxPropertyBag();
                        foreach (var property in sub.Elements().Where(e => e.Name.LocalName == "Property"))
                        {
                            bag.Add(NameOf(property), ValueOf(property));
                        }

                        collection.Items.Add(bag);
                    }
                }
            }

            return record;
        }

        private static string SectionFor(string elementName)
        {
            switch (elementName)
            {
                case "Job": return "DSJOB";
                case "SharedContainer": return "DSSHAREDCONTAINER";
                case "TableDefinitions": return "DSTABLEDEFS";
                case "Routines": return "DSROUTINES";
                case "StageTypes": return "DSSTAGETYPES";
                case "DataElements": return "DSDATAELEMENTS";
                case "Transforms": return "DSTRANSFORMS";
                case "ParameterSets": return "DSPARAMETERSETS";
                default: return "DS" + elementName.ToUpperInvariant();
            }
        }

        private static string NameOf(XElement property) => (string?)property.Attribute("Name") ?? string.Empty;

        private static string ValueOf(XElement property)
        {
            var value = property.Value;
            // Pre-formatted (multi-line) values keep Windows line ends in some exports.
            if (value.IndexOf('\r') >= 0) value = value.Replace("\r\n", "\n");
            return DsxText.DecodeControlEscapes(value);
        }

        private static int LineOf(XObject node) => node is IXmlLineInfo info && info.HasLineInfo() ? info.LineNumber : 0;
    }
}
