#!/usr/bin/env python3
"""
XML to MongoDB Converter Module

This module imports archive.xml files, converts them to JSON format,
and saves the data to MongoDB.

Author: AI Assistant
Date:  2025-01-06
"""

import xml.etree.ElementTree as ET
import json
import re
from typing import Dict, List, Any, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, DuplicateKeyError
import logging
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class XMLToMongoDBConverter:
    """
    Converts XML archive files to JSON and stores them in MongoDB
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize the converter with MongoDB connection details
        """
        self.connection_string = connection_string
        
    def clean_xml_content(self, xml_content: str) -> str:
        """
        Clean XML content by removing SQL fragments and fixing malformed XML
        
        Args:
            xml_content: Raw XML content string
            
        Returns:
            str: Cleaned XML content
        """
        # Remove SQL fragments that might be appended to XML
        # Look for patterns like '), 1500000013, '2008-06-15 14:32:56'...
        sql_pattern = r"'\s*,\s*\d+\s*,\s*'[^']*'.*$"
        cleaned_content = re.sub(sql_pattern, "", xml_content, flags=re.MULTILINE | re.DOTALL)
        
        # Ensure proper XML closing
        if not cleaned_content.strip().endswith('</ar:archive>'):
            if '</ar:archive>' in cleaned_content:
                # Find the last proper closing tag
                last_close = cleaned_content.rfind('</ar:archive>')
                cleaned_content = cleaned_content[:last_close + len('</ar:archive>')]
        
        return cleaned_content.strip()
    

    def xml_element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """
        Convert XML element to dictionary recursively
        
        Args:
            element: XML element to convert
            
        Returns:
            dict: Dictionary representation of XML element
        """
        result = {}
        
        # Add attributes
        if element.attrib:
            result.update({k.replace('-', '_'): self.check_type(v) for k, v in element.attrib.items()})
        
        # Handle text content
        if element.text and element.text.strip():
            if len(element) == 0:  # Leaf node with text
                return element.text.strip()
            else:
                result['_text'] = element.text.strip()
        
        # Handle child elements
        children_dict = {}
        for child in element:
            child_tag = child.tag
            # Remove namespace prefixes for cleaner JSON
            if ':' in child_tag:
                child_tag = child_tag.split(':', 1)[1]
            
            child_data = self.xml_element_to_dict(child)
            
            # Handle multiple children with same tag
            if child_tag in children_dict:
                if not isinstance(children_dict[child_tag], list):
                    children_dict[child_tag] = [children_dict[child_tag]]
                children_dict[child_tag].append(child_data)
            else:
                children_dict[child_tag] = child_data
        
        result.update(children_dict)
        return result
    
    def check_type(self, value):
        """
        Check if value is a boolean or numeric and convert accordingly
        """
        if value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        elif value.isdigit():
            return int(value)
        elif self.is_datestring(value):
            return self.try_date_formats(value)
        else:
            return value
    
    def is_datestring(self, dstring):
        """
        Check if value is a date string and convert accordingly
        """
        pattern = r"\d{4}-\d{2}-\d{2}"
        dates = re.findall(pattern, dstring)
        if dates:
            return True
        else:
            return False
    
    def try_date_formats(self, date_string):
        """
        Try different date formats
        """
        formats = ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"]
        for date_format in formats:
            try:
                newt = datetime.strptime(date_string, date_format)
                return newt
            except ValueError:
                continue
        return date_string
        
    def parse_xml_file(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parse XML file and extract archive records
        
        Args:
            file_path: Path to the XML file
            
        Returns:
            list: List of parsed archive records as dictionaries
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            # Clean the content
            content = self.clean_xml_content(content)
            
            # Split into individual archive records
            archive_records = []
            
            # Find all <ar:archive> elements
            archive_pattern = r'<ar:archive[^>]*>.*?</ar:archive>'
            matches = re.findall(archive_pattern, content, re.DOTALL)
            
            logger.info(f"Found {len(matches)} archive records in XML file")
            
            for i, match in enumerate(matches):
                try:
                    # Parse individual archive
                    root = ET.fromstring(match)
                    archive_dict = self.xml_element_to_dict(root)
                    
                    # Add metadata
                    archive_dict['_metadata'] = {
                        'record_number': i + 1,
                        'processed_at': datetime.utcnow().isoformat(),
                        'source_file': os.path.basename(file_path),
                        'source_xml': match
                    }
                    
                    archive_records.append(archive_dict)
                    logger.info(f"Successfully parsed archive record {i + 1}")
                    
                #except ET.XMLSyntaxError as e:
                #    logger.error(f"XML parsing error for record {i + 1}: {e}")
                #    continue
                except Exception as e:
                    logger.error(f"Unexpected error parsing record {i + 1}: {e}")
                    continue
            
            return archive_records
            
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            return []
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return []
    
    def save_to_mongodb(self, coll, records: List[Dict[str, Any]]) -> bool:
        """
        Save parsed records to MongoDB
        
        Args:
            coll: reference to mongodb collection
            records: List of record dictionaries to save
            
        Returns:
            bool: True if all records saved successfully
        """
        if not records:
            logger.warning("No records to save")
            return False
        
        try:
            # Insert records
            result = coll.insert_many(records, ordered=False)
            logger.info(f"Successfully inserted {len(result.inserted_ids)} records into MongoDB")
            return True
            
        except DuplicateKeyError as e:
            logger.warning(f"Some records already exist in database: {e}")
            return True  # Partial success is still success
        except Exception as e:
            logger.error(f"Error saving to MongoDB: {e}")
            return False
    
    def process_xml_file(self, collection, file_path: str) -> bool:
        """
        Complete workflow: parse XML file and save to MongoDB
        
        Args:
            file_path: Path to the XML file to process
            
        Returns:
            bool: True if process completed successfully
        """
        logger.info(f"Starting processing of XML file: {file_path}")
        
        try:
            # Parse XML file
            records = self.parse_xml_file(file_path)
            
            if not records:
                logger.error("No records parsed from XML file")
                return False
            
            # Save to MongoDB
            #success = self.save_to_mongodb(collection, records)
            
            #if success:
            #    logger.info(f"Successfully processed {len(records)} records from {file_path}")
            
            return records
            
        except Exception as e:
            logger.error(f"Unexpected error processing XML file: {e}")
            return False

def main():
    """
    Main function to demonstrate usage
    """
    # Configuration
    CONNECTION_STRING = "mongodb+srv://main_admin:xxxxxx@8-native.vmwqj.mongodb.net"
    XML_FILE_PATH = "archive.xml"
    
    # Create converter instance
    converter = XMLToMongoDBConverter(CONNECTION_STRING)
    
    # Process the XML file
    success = converter.process_xml_file(XML_FILE_PATH)
    
    if success:
        print("✅ XML file successfully processed and saved to MongoDB!")
    else:
        print("❌ Failed to process XML file")


if __name__ == "__main__":
    main()
