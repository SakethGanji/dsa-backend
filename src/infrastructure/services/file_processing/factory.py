"""File parser factory implementation."""

from typing import Dict, List
from src.core.abstractions.services import IFileParser, IFileProcessingService
from .parsers import CSVParser, ParquetParser, ExcelParser


class FileParserFactory(IFileProcessingService):
    """Factory for creating appropriate file parsers."""
    
    def __init__(self):
        self._parsers: List[IFileParser] = []
        
        # Register default parsers
        self.register_parser(CSVParser())
        self.register_parser(ParquetParser())
        self.register_parser(ExcelParser())
    
    def get_parser(self, filename: str) -> IFileParser:
        """
        Get the appropriate parser for the given filename.
        
        Args:
            filename: The filename to parse
            
        Returns:
            Appropriate parser instance
            
        Raises:
            ValueError: If no parser supports the file type
        """
        for parser in self._parsers:
            if parser.can_parse(filename):
                return parser
        
        supported = self.list_supported_formats()
        raise ValueError(
            f"Unsupported file type for '{filename}'. "
            f"Supported formats: {supported}"
        )
    
    def register_parser(self, parser: IFileParser) -> None:
        """Register a new parser with the factory."""
        self._parsers.append(parser)
    
    def list_supported_formats(self) -> Dict[str, List[str]]:
        """
        List all supported formats and their extensions.
        
        Returns:
            Dict mapping parser name to list of extensions
        """
        formats = {}
        for parser in self._parsers:
            parser_name = parser.__class__.__name__
            formats[parser_name] = parser.get_supported_extensions()
        return formats