"""Unit tests for file parser factory."""

import pytest
from unittest.mock import Mock, MagicMock

from src.core.infrastructure.services.file_processing.factory import FileParserFactory
from src.core.infrastructure.services.file_processing.parsers import (
    CSVParser, ParquetParser, ExcelParser
)
from src.core.abstractions.services import IFileParser


class TestFileParserFactory:
    """Test file parser factory implementation."""
    
    @pytest.fixture
    def factory(self):
        return FileParserFactory()
    
    def test_factory_initialization(self, factory):
        """Test factory is initialized with default parsers."""
        # Check that default parsers are registered
        formats = factory.list_supported_formats()
        assert 'CSVParser' in formats
        assert 'ParquetParser' in formats
        assert 'ExcelParser' in formats
    
    def test_get_parser_csv(self, factory):
        """Test getting CSV parser."""
        parser = factory.get_parser("data.csv")
        assert isinstance(parser, CSVParser)
        
        parser = factory.get_parser("DATA.CSV")
        assert isinstance(parser, CSVParser)
    
    def test_get_parser_parquet(self, factory):
        """Test getting Parquet parser."""
        parser = factory.get_parser("data.parquet")
        assert isinstance(parser, ParquetParser)
    
    def test_get_parser_excel(self, factory):
        """Test getting Excel parser."""
        parser = factory.get_parser("data.xlsx")
        assert isinstance(parser, ExcelParser)
        
        parser = factory.get_parser("data.xls")
        assert isinstance(parser, ExcelParser)
    
    def test_get_parser_unsupported_format(self, factory):
        """Test error for unsupported format."""
        with pytest.raises(ValueError, match="Unsupported file type"):
            factory.get_parser("data.txt")
        
        with pytest.raises(ValueError, match="Unsupported file type"):
            factory.get_parser("data.json")
    
    def test_register_custom_parser(self, factory):
        """Test registering a custom parser."""
        # Create a mock parser
        mock_parser = Mock(spec=IFileParser)
        mock_parser.can_parse.side_effect = lambda f: f.endswith('.custom')
        mock_parser.get_supported_extensions.return_value = ['.custom']
        
        # Register the parser
        factory.register_parser(mock_parser)
        
        # Should now be able to get this parser
        parser = factory.get_parser("data.custom")
        assert parser == mock_parser
        mock_parser.can_parse.assert_called_with("data.custom")
    
    def test_list_supported_formats(self, factory):
        """Test listing all supported formats."""
        formats = factory.list_supported_formats()
        
        assert isinstance(formats, dict)
        assert len(formats) >= 3  # At least CSV, Parquet, Excel
        
        # Check specific parsers
        assert '.csv' in formats['CSVParser']
        assert '.parquet' in formats['ParquetParser']
        assert '.xlsx' in formats['ExcelParser']
        assert '.xls' in formats['ExcelParser']
    
    def test_parser_priority(self, factory):
        """Test that last registered parser takes priority."""
        # Create two parsers that handle the same extension
        parser1 = Mock(spec=IFileParser)
        parser1.can_parse.side_effect = lambda f: f.endswith('.test')
        parser1.get_supported_extensions.return_value = ['.test']
        
        parser2 = Mock(spec=IFileParser)
        parser2.can_parse.side_effect = lambda f: f.endswith('.test')
        parser2.get_supported_extensions.return_value = ['.test']
        
        # Register both
        factory.register_parser(parser1)
        factory.register_parser(parser2)
        
        # Should get the last registered parser
        parser = factory.get_parser("data.test")
        assert parser == parser2