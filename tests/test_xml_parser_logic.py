import unittest
import sys
from pathlib import Path
import pandas as pd

# Add the project root to the Python path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from modules.xml_parser_logic import clean_html_content, _parse_xsd_for_docs, parse_xml_to_dataframe

class TestXMLParserLogic(unittest.TestCase):

    # --- Tests for clean_html_content ---

    def test_clean_html_simple_tags(self):
        """Test cleaning of simple HTML tags like <b> and <i>."""
        content = "<b>Bold Text</b> and <i>Italic Text</i>"
        expected = "** Bold Text ** and *Italic Text*"
        self.assertEqual(clean_html_content(content), expected)

    def test_clean_html_cdata(self):
        """Test extraction of content inside CDATA sections."""
        content = "<![CDATA[<p>Content inside CDATA</p>]]>"
        # <p> converts to \n\n, then stripped
        result = clean_html_content(content)
        self.assertIn("Content inside CDATA", result)

    def test_clean_html_lists(self):
        """Test formatting of unordered lists."""
        content = "<ul><li>Item 1</li><li>Item 2</li></ul>"
        result = clean_html_content(content)
        self.assertIn("• Item 1", result)
        self.assertIn("• Item 2", result)

    def test_clean_html_ordered_lists(self):
        """Test formatting of ordered lists (<ol>)."""
        content = "<ol><li>First</li><li>Second</li></ol>"
        result = clean_html_content(content)
        # Implementation converts <li> to bullet points regardless of parent list type
        self.assertIn("• First", result)
        self.assertIn("• Second", result)

    def test_clean_html_strong_em(self):
        """Test formatting of <strong> and <em> tags."""
        content = "<strong>Strong</strong> and <em>Emphasized</em>"
        expected = "** Strong ** and *Emphasized*"
        self.assertEqual(clean_html_content(content), expected)

    def test_clean_html_headers(self):
        """Test formatting of header tags."""
        content = "<h1>Title</h1>"
        result = clean_html_content(content)
        self.assertIn("** Title **", result)

    def test_clean_html_empty(self):
        """Test handling of empty or None input."""
        self.assertEqual(clean_html_content(""), "")
        self.assertEqual(clean_html_content(None), "")

    def test_clean_html_code_blocks(self):
        """Test formatting of <pre> and <code> tags."""
        content = "<pre>def foo():\n    pass</pre> and <code>var x</code>"
        result = clean_html_content(content)
        # Note: The implementation strips indentation from lines
        self.assertIn("def foo():", result)
        self.assertIn("pass", result)
        self.assertIn("`var x`", result)

    def test_clean_html_spacing_fixes(self):
        """Test that spacing is fixed around bullets and bold text."""
        # The function adds spaces after bullets and around bold markers
        content = "<ul><li>Item</li></ul><b>Bold</b>"
        result = clean_html_content(content)
        self.assertIn("• Item", result)
        self.assertIn("** Bold **", result)

    # --- Tests for _parse_xsd_for_docs ---

    def test_parse_xsd_valid(self):
        """Test parsing a valid XSD string for documentation."""
        xsd_content = """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="MyParam">
                <xs:annotation>
                    <xs:documentation>My Description</xs:documentation>
                </xs:annotation>
            </xs:element>
        </xs:schema>
        """
        docs = _parse_xsd_for_docs(xsd_content)
        self.assertIn("MyParam", docs)
        self.assertEqual(docs["MyParam"], "My Description")

    def test_parse_xsd_with_html_in_doc(self):
        """Test that HTML inside XSD documentation is cleaned."""
        xsd_content = """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="HtmlParam">
                <xs:annotation>
                    <xs:documentation><b>Bold</b> Desc</xs:documentation>
                </xs:annotation>
            </xs:element>
        </xs:schema>
        """
        docs = _parse_xsd_for_docs(xsd_content)
        self.assertEqual(docs["HtmlParam"], "** Bold ** Desc")

    def test_parse_xsd_custom_namespace(self):
        """Test parsing XSD with non-standard namespace prefixes."""
        xsd_content = """
        <custom:schema xmlns:custom="http://www.w3.org/2001/XMLSchema">
            <custom:element name="CustomParam">
                <custom:annotation>
                    <custom:documentation>Custom Namespace Doc</custom:documentation>
                </custom:annotation>
            </custom:element>
        </custom:schema>
        """
        docs = _parse_xsd_for_docs(xsd_content)
        self.assertEqual(docs.get("CustomParam"), "Custom Namespace Doc")

    def test_parse_xsd_no_documentation(self):
        """Test elements without documentation are ignored or handled."""
        xsd_content = """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="NoDocParam" type="xs:string"/>
        </xs:schema>
        """
        docs = _parse_xsd_for_docs(xsd_content)
        self.assertNotIn("NoDocParam", docs)

    def test_parse_xsd_invalid_xml(self):
        """Test handling of invalid XSD content."""
        docs = _parse_xsd_for_docs("<invalid>xml")
        self.assertEqual(docs, {})

    # --- Tests for parse_xml_to_dataframe ---

    def test_parse_xml_simple_values(self):
        """Test parsing a simple XML into a DataFrame."""
        xml_content = "<root><Param1>Value1</Param1><Param2>Value2</Param2></root>"
        df = parse_xml_to_dataframe(xml_content, "test.xml")
        
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2)
        
        row1 = df[df['Parameter'] == 'Param1'].iloc[0]
        self.assertEqual(row1['Value'], 'Value1')
        
        row2 = df[df['Parameter'] == 'Param2'].iloc[0]
        self.assertEqual(row2['Value'], 'Value2')

    def test_parse_xml_with_xsd_integration(self):
        """Test that XSD documentation is merged into the DataFrame."""
        xml_content = "<root><ConfigParam>100</ConfigParam></root>"
        xsd_content = """
        <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
            <xs:element name="ConfigParam">
                <xs:annotation>
                    <xs:documentation>Sets the configuration value.</xs:documentation>
                </xs:annotation>
            </xs:element>
        </xs:schema>
        """
        df = parse_xml_to_dataframe(xml_content, "test.xml", xsd_content=xsd_content)
        
        row = df[df['Parameter'] == 'ConfigParam'].iloc[0]
        self.assertEqual(row['Value'], '100')
        self.assertEqual(row['Details'], 'Sets the configuration value.')

    def test_parse_xml_skip_system_elements(self):
        """Test that 'root', 'config', and 'schema' elements are skipped."""
        xml_content = """
        <root>
            <config>ignored</config>
            <schema>ignored</schema>
            <ValidParam>KeepMe</ValidParam>
        </root>
        """
        df = parse_xml_to_dataframe(xml_content, "test.xml")
        params = df['Parameter'].tolist()
        self.assertNotIn("config", params)
        self.assertNotIn("schema", params)
        self.assertIn("ValidParam", params)

    def test_parse_xml_splitter_logic(self):
        """Test that elements ending in _SPLITTER are kept even if empty."""
        xml_content = "<root><MY_SPLITTER></MY_SPLITTER><EmptyParam></EmptyParam></root>"
        df = parse_xml_to_dataframe(xml_content, "test.xml")
        
        params = df['Parameter'].tolist()
        self.assertIn("MY_SPLITTER", params)
        # EmptyParam has no value and is not a splitter, so it should be skipped
        self.assertNotIn("EmptyParam", params)

    def test_parse_xml_empty_content(self):
        """Test handling of empty XML content."""
        df = parse_xml_to_dataframe("", "test.xml")
        self.assertTrue(df.empty)

    def test_parse_xml_malformed(self):
        """Test that the parser recovers from malformed XML content."""
        xml_content = "<root><OpenTag>Value</root>" # Missing closing tag
        df = parse_xml_to_dataframe(xml_content, "malformed.xml")
        
        # Because recover=True is used, this should actually succeed
        self.assertFalse(df.empty)
        self.assertEqual(df.iloc[0]['Parameter'], 'OpenTag')
        self.assertEqual(df.iloc[0]['Value'], 'Value')

    def test_parse_xml_with_invalid_xsd_content(self):
        """Test that invalid XSD content is ignored during parsing."""
        xml_content = "<root><Param>Value</Param></root>"
        # Invalid XSD that would cause parsing errors if not handled
        xsd_content = "<xs:schema>... incomplete ..." 
        
        df = parse_xml_to_dataframe(xml_content, "test.xml", xsd_content=xsd_content)
        
        # Should still parse the XML correctly, just without docs
        self.assertFalse(df.empty)
        row = df[df['Parameter'] == 'Param'].iloc[0]
        self.assertEqual(row['Value'], 'Value')

if __name__ == '__main__':
    unittest.main()