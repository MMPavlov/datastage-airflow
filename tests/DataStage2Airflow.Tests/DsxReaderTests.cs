using System.IO;
using System.Linq;
using System.Text;
using DataStage2Airflow.Dsx;
using Xunit;

namespace DataStage2Airflow.Tests
{
    public class DsxReaderTests
    {
        private const string Minimal = @"BEGIN HEADER
   CharacterSet ""CP1252""
   ExportingTool ""Ascential DataStage Export""
   ToolInstanceID ""PROJ""
   ServerVersion ""7.5.1""
END HEADER
BEGIN DSJOB
   Identifier ""J1""
   DateModified ""2005-01-01""
   BEGIN DSRECORD
      Identifier ""ROOT""
      OLEType ""CJobDefn""
      Name ""J1""
      FullDescription =+=+=+=
line one
  line two
=+=+=+=
      Parameters ""CParameters""
      BEGIN DSSUBRECORD
         Name ""P1""
         Default ""a \""quoted\"" \\ value""
      END DSSUBRECORD
      BEGIN DSSUBRECORD
         Name ""P2""
      END DSSUBRECORD
      JobType ""3""
   END DSRECORD
   BEGIN DSRECORD
      Identifier ""V0S0P1""
      OLEType ""CCustomOutput""
      Name ""lnk""
      Properties ""CCustomProperty""
      BEGIN DSSUBRECORD
         Name ""file ""
         Value ""\(2)\(2)0\(1)\(3)file \(2)/in/a.txt\(2)0\(1)\(3)file \(2)/in/b.txt\(2)0""
      END DSSUBRECORD
      BEGIN DSSUBRECORD
         Name ""append\\overwrite""
         Value ""overwrite""
      END DSSUBRECORD
      Columns ""COutputColumn""
      BEGIN DSSUBRECORD
         Name ""C1""
         SqlType ""12""
      END DSSUBRECORD
      StageVars ""JobID""
   END DSRECORD
END DSJOB
";

        private const char Entry = (char)1;
        private const char Field = (char)2;
        private const char Level = (char)3;

        [Fact]
        public void Reads_header_objects_and_records()
        {
            var document = DsxReader.Parse(Minimal);
            Assert.Equal("PROJ", document.ProjectName);
            Assert.Equal("7.5.1", document.ServerVersion);
            var job = Assert.Single(document.Jobs);
            Assert.Equal("J1", job.Identifier);
            Assert.Equal("2005-01-01", job.Properties["DateModified"]);
            Assert.Equal(2, job.Records.Count);
            Assert.Equal("CJobDefn", job.Root!.OleType);
        }

        [Fact]
        public void Multi_line_values_keep_their_lines()
        {
            var root = DsxReader.Parse(Minimal).Jobs.Single().Root!;
            Assert.Equal("line one\n  line two", root["FullDescription"]);
        }

        [Fact]
        public void Quoted_strings_unescape_quotes_backslashes_and_hex_codes()
        {
            var job = DsxReader.Parse(Minimal).Jobs.Single();
            Assert.Equal("a \"quoted\" \\ value", job.Root!.SubRecords("Parameters")[0]["Default"]);
            var pin = job.FindRecord("V0S0P1")!;
            Assert.Equal("append\\overwrite", pin.SubRecords("Properties")[1]["Name"]);
            Assert.Contains(Field, pin.SubRecords("Properties")[0]["Value"]!);
        }

        [Fact]
        public void Property_lines_followed_by_subrecords_form_collections()
        {
            var pin = DsxReader.Parse(Minimal).Jobs.Single().FindRecord("V0S0P1")!;
            Assert.Equal(2, pin.SubRecords("Properties").Count);
            Assert.Equal("COutputColumn", pin.Collection("Columns")!.TypeName);
            Assert.Single(pin.SubRecords("Columns"));
            Assert.Null(pin.Collection("StageVars"));
            Assert.Equal("JobID", pin["StageVars"]);
        }

        [Fact]
        public void Encoded_property_values_decode_to_trees()
        {
            var pin = DsxReader.Parse(Minimal).Jobs.Single().FindRecord("V0S0P1")!;
            var nodes = PropertyTree.Parse("file", pin.SubRecords("Properties")[0]["Value"]!);
            Assert.Equal(new[] { "/in/a.txt", "/in/b.txt" }, nodes.Select(n => n.Value).ToArray());
            Assert.All(nodes, n => Assert.Equal("file", n.Name));
        }

        [Fact]
        public void Nested_property_trees_attach_children()
        {
            var value = string.Concat(
                Field, Field, "0",
                Entry, Level, "reduce", Field, "AMOUNT", Field, "0",
                Entry, Level, Level, "sum", Field, "TOTAL", Field, "0",
                Entry, Level, Level, "max", Field, "TOP", Field, "0",
                Entry, Level, "reduce", Field, "QTY", Field, "0");
            var nodes = PropertyTree.Parse("reduce", value);
            Assert.Equal(2, nodes.Count);
            Assert.Equal("TOTAL", nodes[0].ChildValue("sum"));
            Assert.Equal("TOP", nodes[0].ChildValue("max"));
            Assert.Empty(nodes[1].Children);
        }

        [Fact]
        public void Structural_problems_become_diagnostics()
        {
            var diagnostics = new DiagnosticBag();
            var text = "BEGIN HEADER\n ToolInstanceID \"P\"\nEND HEADER\nBEGIN DSJOB\n Identifier \"J\"\n BEGIN DSRECORD\n  Identifier \"ROOT\"\nEND DSJOB\nEND DSRECORD\n";
            var document = DsxReader.Parse(text, "broken.dsx", diagnostics);
            Assert.Single(document.Jobs);
            Assert.NotEqual(0, diagnostics.WarningCount);
        }

        [Fact]
        public void Text_that_is_not_an_export_is_rejected()
        {
            Assert.Throws<DsxFormatException>(() => DsxReader.Parse("hello world\n"));
        }

        [Fact]
        public void Osh_record_format_is_parsed()
        {
            var format = OshFormat.Parse("final_delim=end, delim=',', null_field=\"\", quote=none, record_delim='\\n'");
            Assert.Equal(",", format["delim"]);
            Assert.Equal(string.Empty, format["null_field"]);
            Assert.Equal("none", format["quote"]);
            Assert.Equal("\n", format["record_delim"]);
            Assert.Null(OshFormat.Symbol("none"));
            Assert.Equal("\"", OshFormat.Symbol("double"));
        }

        [Fact]
        public void Cp1252_files_are_decoded_when_they_are_not_utf8()
        {
            var bytes = Encoding.ASCII.GetBytes("BEGIN HEADER\n ToolInstanceID \"PROJ\"\nEND HEADER\nBEGIN DSJOB\n Identifier \"Caf")
                .Concat(new byte[] { 0xE9, 0x80 })
                .Concat(Encoding.ASCII.GetBytes("\"\nEND DSJOB\n")).ToArray();
            var path = Path.Combine(TestPaths.NewTempDirectory("cp1252"), "cp1252.dsx");
            File.WriteAllBytes(path, bytes);
            var document = ExportReader.ReadFile(path);
            Assert.Equal("Caf" + (char)0xE9 + (char)0x20AC, document.Jobs.Single().Identifier);
        }

        [Fact]
        public void Xml_exports_map_to_the_same_model()
        {
            var xml = @"<?xml version=""1.0"" encoding=""UTF-8""?>
<DSExport>
  <Header CharacterSet=""CP1252"" ExportingTool=""Ascential DataStage Export"" ToolInstanceID=""PROJ"" ServerVersion=""7.5.1""/>
  <Job Identifier=""J1"" DateModified=""2005-01-01"">
    <Record Identifier=""ROOT"" Type=""JobDefn"" Readonly=""0"">
      <Property Name=""Name"">J1</Property>
      <Property Name=""JobType"">0</Property>
      <Collection Name=""Parameters"" Type=""Parameters"">
        <SubRecord><Property Name=""Name"">P1</Property><Property Name=""Default"">x</Property></SubRecord>
      </Collection>
    </Record>
    <Record Identifier=""V0S0"" Type=""CustomStage"" Readonly=""0"">
      <Property Name=""Name"">S1</Property>
      <Property Name=""Value"">\(2)\(2)0\(1)\(3)file\(2)/a.txt\(2)0</Property>
    </Record>
  </Job>
</DSExport>";
            var document = ExportReader.ReadText(xml, "export.xml");
            Assert.Equal(ExportFormat.Xml, document.Format);
            Assert.Equal("PROJ", document.ProjectName);
            var job = document.Jobs.Single();
            Assert.Equal("CJobDefn", job.Root!.OleType);
            Assert.Equal("x", job.Root.SubRecords("Parameters")[0]["Default"]);
            Assert.Equal("CCustomStage", job.FindRecord("V0S0")!.OleType);
            Assert.Contains(Level, job.FindRecord("V0S0")!["Value"]!);
        }
    }
}
