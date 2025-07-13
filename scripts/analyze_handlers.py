#!/usr/bin/env python3
"""
Analyze handlers to determine which ones should use base classes.
"""

import os
import re
from pathlib import Path
from typing import Dict, List, Set
import ast


class HandlerAnalyzer(ast.NodeVisitor):
    """Analyze Python AST to understand handler patterns."""
    
    def __init__(self):
        self.handlers = {}
        self.current_class = None
        self.current_methods = []
        self.has_handle_method = False
        self.has_db_operations = False
        self.operation_type = None
        self.complexity_score = 0
        
    def visit_ClassDef(self, node):
        if node.name.endswith('Handler'):
            self.current_class = node.name
            self.current_methods = []
            self.has_handle_method = False
            self.has_db_operations = False
            self.complexity_score = 0
            self.operation_type = self._guess_operation_type(node.name)
            
            # Visit all methods in the class
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    self.current_methods.append(item.name)
                    if item.name == 'handle':
                        self.has_handle_method = True
                        self.complexity_score = self._calculate_complexity(item)
                    
                    # Check for database operations
                    for n in ast.walk(item):
                        if isinstance(n, ast.Attribute):
                            if any(repo in str(n.attr) for repo in ['_repo', 'repository', '_dataset_repo', '_user_repo']):
                                self.has_db_operations = True
            
            self.handlers[self.current_class] = {
                'methods': self.current_methods,
                'has_handle': self.has_handle_method,
                'has_db': self.has_db_operations,
                'operation_type': self.operation_type,
                'complexity': self.complexity_score
            }
            
        self.generic_visit(node)
    
    def _guess_operation_type(self, handler_name: str) -> str:
        """Guess the operation type from handler name."""
        name_lower = handler_name.lower()
        
        if 'create' in name_lower:
            return 'create'
        elif 'update' in name_lower:
            return 'update'
        elif 'delete' in name_lower:
            return 'delete'
        elif 'list' in name_lower or 'get' in name_lower:
            return 'read'
        elif 'import' in name_lower or 'export' in name_lower:
            return 'io'
        elif 'process' in name_lower or 'execute' in name_lower:
            return 'process'
        elif 'search' in name_lower:
            return 'search'
        elif 'calculate' in name_lower or 'analyze' in name_lower:
            return 'analytics'
        else:
            return 'custom'
    
    def _calculate_complexity(self, node: ast.FunctionDef) -> int:
        """Calculate complexity score for a method."""
        complexity = 0
        
        # Count control flow statements
        for n in ast.walk(node):
            if isinstance(n, (ast.If, ast.For, ast.While)):
                complexity += 1
            elif isinstance(n, ast.Try):
                complexity += 2
            elif isinstance(n, ast.AsyncWith):
                complexity += 1
        
        # Count lines
        if hasattr(node, 'lineno') and hasattr(node, 'end_lineno'):
            lines = node.end_lineno - node.lineno
            complexity += lines // 10
        
        return complexity


def analyze_file(filepath: Path) -> Dict:
    """Analyze a single Python file for handlers."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        tree = ast.parse(content)
        analyzer = HandlerAnalyzer()
        analyzer.visit(tree)
        
        return analyzer.handlers
    except Exception as e:
        print(f"Error analyzing {filepath}: {e}")
        return {}


def categorize_handlers(all_handlers: Dict[str, Dict]) -> Dict[str, List[str]]:
    """Categorize handlers by their suitability for base classes."""
    categories = {
        'use_base_update': [],
        'use_base_delete': [],
        'use_base_create': [],
        'use_pagination': [],
        'keep_custom': [],
        'review_needed': []
    }
    
    for handler_name, info in all_handlers.items():
        operation = info['operation_type']
        complexity = info['complexity']
        
        # Simple update handlers
        if operation == 'update' and complexity < 10:
            categories['use_base_update'].append(handler_name)
        
        # Simple delete handlers
        elif operation == 'delete' and complexity < 5:
            categories['use_base_delete'].append(handler_name)
        
        # Simple create handlers
        elif operation == 'create' and complexity < 10 and 'file' not in handler_name.lower():
            categories['use_base_create'].append(handler_name)
        
        # List/read handlers with pagination
        elif operation == 'read' and 'list' in handler_name.lower():
            categories['use_pagination'].append(handler_name)
        
        # Complex operations that should stay custom
        elif operation in ['io', 'process', 'search', 'analytics'] or complexity > 15:
            categories['keep_custom'].append(handler_name)
        
        # Needs manual review
        else:
            categories['review_needed'].append(handler_name)
    
    return categories


def main():
    # Find all handler files
    handler_files = []
    for filepath in Path('./src/features').rglob('*.py'):
        if '__pycache__' not in str(filepath) and 'handler' in str(filepath).lower():
            handler_files.append(filepath)
    
    print(f"Found {len(handler_files)} handler files to analyze")
    print()
    
    # Analyze all handlers
    all_handlers = {}
    for filepath in handler_files:
        handlers = analyze_file(filepath)
        for handler_name, info in handlers.items():
            info['file'] = str(filepath)
            all_handlers[handler_name] = info
    
    # Categorize handlers
    categories = categorize_handlers(all_handlers)
    
    # Print results
    print("Handler Analysis Results")
    print("=" * 50)
    print()
    
    print("1. SHOULD USE BaseUpdateHandler:")
    for handler in sorted(categories['use_base_update']):
        print(f"   - {handler} ({all_handlers[handler]['file']})")
    print()
    
    print("2. SHOULD USE BaseDeleteHandler (or pattern):")
    for handler in sorted(categories['use_base_delete']):
        print(f"   - {handler} ({all_handlers[handler]['file']})")
    print()
    
    print("3. SHOULD USE BaseCreateHandler (when created):")
    for handler in sorted(categories['use_base_create']):
        print(f"   - {handler} ({all_handlers[handler]['file']})")
    print()
    
    print("4. SHOULD USE PaginationMixin:")
    for handler in sorted(categories['use_pagination']):
        print(f"   - {handler} ({all_handlers[handler]['file']})")
    print()
    
    print("5. KEEP CUSTOM (complex business logic):")
    for handler in sorted(categories['keep_custom']):
        info = all_handlers[handler]
        print(f"   - {handler} (type: {info['operation_type']}, complexity: {info['complexity']})")
    print()
    
    print("6. NEEDS MANUAL REVIEW:")
    for handler in sorted(categories['review_needed']):
        info = all_handlers[handler]
        print(f"   - {handler} (type: {info['operation_type']}, complexity: {info['complexity']})")
    print()
    
    # Summary
    total = len(all_handlers)
    can_use_base = len(categories['use_base_update'] + categories['use_base_delete'] + categories['use_base_create'])
    keep_custom = len(categories['keep_custom'])
    
    print("Summary:")
    print(f"  Total handlers: {total}")
    print(f"  Can use base classes: {can_use_base} ({can_use_base/total*100:.1f}%)")
    print(f"  Should stay custom: {keep_custom} ({keep_custom/total*100:.1f}%)")
    print(f"  Needs review: {len(categories['review_needed'])}")


if __name__ == '__main__':
    main()