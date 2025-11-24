#!/usr/bin/env python3
"""
Code Replacer Module

This module replaces codes in a JSON document with their definitions from 
referenced-entity and typekey arrays. Codes starting with 'o' are replaced 
with values from referenced-entity, and codes starting with 't' are replaced 
with values from typekey.

Usage:
    from code_replacer import CodeReplacer
    
    replacer = CodeReplacer('sample_doc.json')
    replaced_doc = replacer.replace_codes()
    replacer.save_replaced_document('output.json', replaced_doc)
"""

import json
import re
from typing import Dict, Any, Union, List
from pathlib import Path


class CodeReplacer:
    """
    A class to replace codes in JSON documents with their definitions.
    
    Attributes:
        original_doc (dict): The original JSON document
        referenced_entities (dict): Mapping of 'o' codes to their definitions
        typekeys (dict): Mapping of 't' codes to their definitions
    """
    
    def __init__(self, json_doc: dict):
        """
        Initialize the CodeReplacer with a JSON file.
        
        Args:
            json_doc (dict): The raw json document (dict)
        """
        self.original_doc = json_doc
        self.referenced_entities = self._build_referenced_entities_map()
        self.typekeys = self._build_typekeys_map()
    
    def _build_referenced_entities_map(self) -> Dict[str, Dict[str, Any]]:
        """
        Build a mapping of referenced entity IDs to their full definitions.
        
        Returns:
            Dict mapping entity IDs (like 'o57') to their definition objects
        """
        entities_map = {}
        referenced_entities = self.original_doc.get('referenced-entity', [])
        
        for entity in referenced_entities:
            entity_id = entity.get('id')
            if entity_id:
                entities_map[entity_id] = entity
        
        return entities_map
    
    def _build_typekeys_map(self) -> Dict[str, Dict[str, Any]]:
        """
        Build a mapping of typekey IDs to their full definitions.
        
        Returns:
            Dict mapping typekey IDs (like 't0') to their definition objects
        """
        typekeys_map = {}
        typekeys = self.original_doc.get('typekey', [])
        
        for typekey in typekeys:
            typekey_id = typekey.get('id')
            if typekey_id:
                typekeys_map[typekey_id] = typekey
        
        return typekeys_map
    
    def _is_code(self, value: Any) -> bool:
        """
        Check if a value is a code that should be replaced.
        
        Args:
            value: The value to check
            
        Returns:
            bool: True if the value is a code (starts with 'o' or 't' followed by digits)
        """
        if not isinstance(value, str):
            return False
        
        # Match pattern: letter followed by digits (e.g., 'o57', 't0')
        return bool(re.match(r'^[ot]\d+$', value))
    
    def _get_replacement_value(self, code: str, replacement_strategy: str = 'full') -> Union[Dict[str, Any], str]:
        """
        Get the replacement value for a given code.

        Args:
            code (str): The code to replace (e.g., 'o57', 't0')
            replacement_strategy (str): Strategy for replacement:
                - 'full': Return full object with all metadata
                - 'display_name': Return only the display name
                - 'code': Return only the code value (for typekeys)
                - 'simple': Return a simplified object with key fields

        Returns:
            Replacement value based on strategy, or the original code if not found
        """
        if code.startswith('o'):
            entity = self.referenced_entities.get(code)
            if entity:
                if replacement_strategy == 'display_name':
                    return f'{entity.get('display_name', code)}|{code}'
                elif replacement_strategy == 'simple':
                    return {
                        'original_code': code,
                        'display_name': entity.get('display_name', ''),
                        'type': entity.get('type', '').split('.')[-1] if entity.get('type') else ''
                    }
                else:  # 'full'
                    return {
                        'original_code': code,
                        'type': entity.get('type', ''),
                        'display_name': entity.get('display_name', ''),
                        'public_id': entity.get('public_id', ''),
                        'ns': entity.get('ns', '')
                    }
        elif code.startswith('t'):
            typekey = self.typekeys.get(code)
            if typekey:
                if replacement_strategy == 'display_name':
                    return typekey.get('display_name', code)
                elif replacement_strategy == 'code':
                    return typekey.get('code', code)
                elif replacement_strategy == 'simple':
                    return {
                        'original_code': code,
                        'code': typekey.get('code', ''),
                        'display_name': typekey.get('display_name', ''),
                        'type': typekey.get('type', '').split('.')[-1] if typekey.get('type') else ''
                    }
                else:  # 'full'
                    return {
                        'original_code': code,
                        'type': typekey.get('type', ''),
                        'code': typekey.get('code', ''),
                        'display_name': typekey.get('display_name', ''),
                        'ns': typekey.get('ns', '')
                    }

        # Return original code if no replacement found
        return code
    
    def _replace_codes_recursive(self, obj: Any, replacement_strategy: str = 'full') -> Any:
        """
        Recursively traverse and replace codes in the document.

        Args:
            obj: The object to process (can be dict, list, or primitive)
            replacement_strategy (str): Strategy for replacement

        Returns:
            The object with codes replaced
        """
        if isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                # Skip the reference sections to avoid circular replacements
                if key in ['referenced-entity', 'typekey']:
                    result[key] = value
                else:
                    result[key] = self._replace_codes_recursive(value, replacement_strategy)
            return result

        elif isinstance(obj, list):
            return [self._replace_codes_recursive(item, replacement_strategy) for item in obj]

        elif self._is_code(obj):
            return self._get_replacement_value(obj, replacement_strategy)

        else:
            return obj
    
    def replace_codes(self, replacement_strategy: str = 'full', include_metadata: bool = True) -> Dict[str, Any]:
        """
        Replace all codes in the document with their definitions.

        Args:
            replacement_strategy (str): Strategy for replacement:
                - 'full': Return full object with all metadata (default)
                - 'display_name': Return only the display name
                - 'code': Return only the code value (for typekeys)
                - 'simple': Return a simplified object with key fields
            include_metadata (bool): Whether to include replacement metadata

        Returns:
            Dict: The document with codes replaced
        """
        replaced_doc = self._replace_codes_recursive(self.original_doc.copy(), replacement_strategy)

        if include_metadata:
            # Add metadata about the replacement process
            replaced_doc['_replacement_metadata'] = {
                'total_referenced_entities': len(self.referenced_entities),
                'total_typekeys': len(self.typekeys),
                'replacement_strategy': replacement_strategy,
                'replacement_timestamp': self._get_current_timestamp()
            }

        return replaced_doc
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def save_replaced_document(self, output_path: str, replaced_doc: Dict[str, Any] = None) -> None:
        """
        Save the replaced document to a file.
        
        Args:
            output_path (str): Path where to save the replaced document
            replaced_doc (Dict): The replaced document (if None, will generate it)
        """
        if replaced_doc is None:
            replaced_doc = self.replace_codes()
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as file:
            json.dump(replaced_doc, file, indent=2, ensure_ascii=False)
        
        print(f"Replaced document saved to: {output_file}")
    
    def get_replacement_summary(self) -> Dict[str, Any]:
        """
        Get a summary of available replacements.
        
        Returns:
            Dict containing summary information
        """
        return {
            'referenced_entities_count': len(self.referenced_entities),
            'typekeys_count': len(self.typekeys),
            'referenced_entity_ids': list(self.referenced_entities.keys()),
            'typekey_ids': list(self.typekeys.keys()),
            'sample_referenced_entities': dict(list(self.referenced_entities.items())[:5]),
            'sample_typekeys': dict(list(self.typekeys.items())[:5])
        }


def main():
    """Example usage of the CodeReplacer."""
    try:
        # Initialize the replacer
        replacer = CodeReplacer('sample_doc.json')
        
        # Print summary
        summary = replacer.get_replacement_summary()
        print("Replacement Summary:")
        print(f"- Referenced entities: {summary['referenced_entities_count']}")
        print(f"- Typekeys: {summary['typekeys_count']}")
        
        # Replace codes and save
        replaced_doc = replacer.replace_codes()
        #replacer.save_replaced_document('sample_doc_replaced.json', replaced_doc)
        
        print("Code replacement completed successfully!")
        return replaced_doc
        
    except Exception as e:
        print(f"Error: {e}")


#if __name__ == "__main__":
#    main()
