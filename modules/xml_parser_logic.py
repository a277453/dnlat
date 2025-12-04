from lxml import etree as ET
import re
import pandas as pd
import html
from modules.logging_config import logger
import logging


logger.info("Starting xml_parser_logic")



def clean_html_content(content: str) -> str:
    """Cleans and formats HTML content from documentation with proper spacing."""
    logger.info("Cleaning HTML content started")
    try:
        if not content:
            logger.debug("Empty content received for cleaning")
            return ""
        
        cdata_match = re.search(r'<!\[CDATA\[(.*?)\]\]>', content, re.DOTALL)
        if cdata_match:
            logger.debug("CDATA section detected in HTML content")
            content = cdata_match.group(1)
        
        content = html.unescape(content)
        logger.debug("HTML entities unescaped")

        # Convert HTML to markdown with spacing
        content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</p>', '\n\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<b[^>]*>(.*?)</b>', r'**\1**', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'<i[^>]*>(.*?)</i>', r'*\1*', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'<em[^>]*>(.*?)</em>', r'*\1*', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'<ul[^>]*>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</ul>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<li[^>]*>', '\n• ', content, flags=re.IGNORECASE)
        content = re.sub(r'</li>', '', content, flags=re.IGNORECASE)
        content = re.sub(r'<ol[^>]*>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</ol>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<h[1-6][^>]*>(.*?)</h[1-6]>', r'\n\n**\1**\n', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'<code[^>]*>(.*?)</code>', r'`\1`', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'<pre[^>]*>(.*?)</pre>', r'\n```\n\1\n```\n', content, flags=re.IGNORECASE | re.DOTALL)
        content = re.sub(r'<[^>]+>', '', content)

        # Clean up whitespace
        lines = content.split('\n')
        cleaned_lines = []
        previous_empty = False
        
        for line in lines:
            stripped = line.strip()
            if stripped:
                cleaned_lines.append(stripped)
                previous_empty = False
            elif not previous_empty:
                cleaned_lines.append('')
                previous_empty = True
        
        result = '\n'.join(cleaned_lines).strip()
        result = re.sub(r'•(\S)', r'• \1', result)
        result = re.sub(r'\*\*(\S)', r'** \1', result)
        result = re.sub(r'(\S)\*\*', r'\1 **', result)

        logger.info("HTML content cleaned successfully")
        return result

    except Exception as e:
        logger.error(f"Failed to clean HTML content: {e}")
        return ""


def _parse_xsd_for_docs(xsd_content: str) -> dict:
    """Parses an XSD file to extract documentation for each element.
    
    Returns a dictionary mapping element names to their HTML documentation strings.
    """
    logger.info("XSD parsing started")
    if not xsd_content:
        logger.debug("Empty XSD content received")
        return {}
    
    try:
        docs = {}
        # Define XML namespaces commonly found in XSD files
        namespaces = {
            'xs': 'http://www.w3.org/2001/XMLSchema',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        }
                
        # Use a lenient parser that can recover from minor errors
        parser = ET.XMLParser(remove_blank_text=True, recover=True, resolve_entities=False)
        root = ET.fromstring(xsd_content.encode('utf-8'), parser=parser)
        logger.debug("XSD content parsed successfully")
        # Update namespaces with any additional ones found in the document
        for prefix, uri in root.nsmap.items():
            if prefix is not None and prefix not in namespaces:
                namespaces[prefix] = uri
         # Find all element definitions in the XSD
        xpath_queries = [
            './/xs:element[@name]',
            './/element[@name]'
        ]
        
        elements = []
        for query in xpath_queries:
            try:
                found = root.xpath(query, namespaces=namespaces)
                elements.extend(found)
            except Exception:
                logger.debug(f"XPath query failed: {query}")

        for element in elements:
            # Get the element's name attribute
            element_name = element.get('name')
            if not element_name:
                continue
             # Clean the name (remove namespace prefixes if present)
            element_name = element_name.split('}')[-1] if '}' in element_name else element_name
            element_name = element_name.split(':')[-1] if ':' in element_name else element_name
            # Look for annotation within this element
            annotation_paths = [
                './xs:annotation',
                './annotation',
                './/xs:annotation',
                './/annotation'
            ]
            
            annotation = None
            for anno_path in annotation_paths:
                try:
                    anns = element.xpath(anno_path, namespaces=namespaces)
                    if anns:
                        annotation = anns[0]
                        break
                except Exception:
                    continue
            
            if annotation is None:
                continue
            # Extract documentation from the annotation
            doc_paths = [
                './xs:documentation',
                './documentation',
                './/xs:documentation',
                './/documentation'
            ]
            
            documentation_element = None
            for doc_path in doc_paths:
                try:
                    doc_elems = annotation.xpath(doc_path, namespaces=namespaces)
                    if doc_elems:
                        documentation_element = doc_elems[0]
                        break
                except Exception:
                    continue
            
            if documentation_element is None:
                continue
            
            # Extract the inner content of the documentation element
            doc_content = ET.tostring(documentation_element, encoding='unicode', method='xml')
            # Remove the outer documentation tag to get just the inner content
            doc_content = re.sub(r'<[^:>]*:?documentation[^>]*>', '', doc_content, count=1)
            doc_content = re.sub(r'</[^:>]*:?documentation>', '', doc_content, count=1)
             # Clean the content using our helper function
            doc_content = clean_html_content(doc_content)
            
            if doc_content:
                docs[element_name] = doc_content

        logger.info("XSD parsing completed successfully")
        return docs
        
    except ET.ParseError as e:
        logger.error(f"XML parsing failed: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error while parsing XSD: {e}")
        return {}


def parse_xml_to_dataframe(xml_content: str, filename: str, xsd_content: str = None) -> pd.DataFrame:
    """Parses XML content and returns a DataFrame with parameters and their details.
    Args:
        xml_content: The XML content to parse
        filename: The source filename
        xsd_content: Optional XSD content for documentation extraction
        
    Returns:
        A pandas DataFrame with columns: Parameter, Value, Details, Source
    """
    logger.info(f"XML to DataFrame parsing started for file: {filename}")
    
    if not xml_content:
        logger.error(f"No XML content provided for file: {filename}")
        return pd.DataFrame()
    
    try:
        # Parse the XML
        parser = ET.XMLParser(remove_blank_text=True, recover=True)
        root = ET.fromstring(xml_content.encode('utf-8'), parser=parser)
        logger.debug("XML content parsed into tree")
                # Extract documentation from XSD if available
        docs_dict = {}
        if xsd_content:
            logger.info("Extracting XSD documentation for XML")
            docs_dict = _parse_xsd_for_docs(xsd_content)
            # Extract data from XML
        records = []
        # Walk through all elements in the XML
        
        for elem in root.iter():
            # Get the element name (parameter name)
            param_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
             # Skip certain system elements
            if param_name in ['root', 'config', 'schema']:
                continue
                    # Check if the element is a splitter
            is_splitter = param_name.upper().endswith('_SPLITTER')
            value = elem.text.strip() if elem.text else ""
             # Skip non-splitter elements that have no value
            if not is_splitter and not value:
                continue
            # Get the value
            # Get documentation from XSD if available
            details = docs_dict.get(param_name, "")
            # Create record
            record = {
                'Parameter': param_name,
                'Value': value,
                'Details': details
            }
            
            records.append(record)

        df = pd.DataFrame(records)
        logger.info(f"XML parsed successfully into DataFrame: {filename}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to parse XML to DataFrame for file {filename}: {e}")
        return pd.DataFrame()