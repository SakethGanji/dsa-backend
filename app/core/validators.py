"""Core validation utilities shared across vertical slices.

This module provides reusable validation functions and classes that can be
used by any vertical slice to ensure consistent validation behavior.
"""
from typing import List, Optional, Tuple, Set
from fastapi import HTTPException, status


class BaseValidator:
    """Base validator with common validation methods."""
    
    @staticmethod
    def validate_pagination(limit: int, offset: int, max_limit: int = 1000) -> Tuple[int, int]:
        """Validate and normalize pagination parameters.
        
        Args:
            limit: Number of items to return
            offset: Number of items to skip
            max_limit: Maximum allowed limit
            
        Returns:
            Tuple of (normalized_limit, normalized_offset)
        """
        if limit < 1:
            limit = 10
        elif limit > max_limit:
            limit = max_limit
            
        if offset < 0:
            offset = 0
            
        return limit, offset
    
    @staticmethod
    def validate_tags(
        tags: Optional[List[str]], 
        max_tags: int = 20,
        max_tag_length: int = 50
    ) -> Optional[List[str]]:
        """Validate and clean a list of tags.
        
        Args:
            tags: List of tag strings to validate
            max_tags: Maximum number of tags allowed
            max_tag_length: Maximum length for each tag
            
        Returns:
            Cleaned list of tags or None if no valid tags
            
        Raises:
            HTTPException: If validation fails
        """
        if not tags:
            return None
        
        if len(tags) > max_tags:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Too many tags. Maximum allowed: {max_tags}"
            )
        
        cleaned_tags = []
        seen_tags = set()  # For deduplication
        
        for tag in tags:
            tag = tag.strip()
            if not tag:
                continue
                
            if len(tag) > max_tag_length:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Tag '{tag}' exceeds maximum length of {max_tag_length} characters"
                )
            
            # Case-insensitive deduplication
            tag_lower = tag.lower()
            if tag_lower not in seen_tags:
                cleaned_tags.append(tag)
                seen_tags.add(tag_lower)
        
        return cleaned_tags if cleaned_tags else None


class FileValidator:
    """Generic file validation utilities."""
    
    @classmethod
    def validate_file_upload(
        cls,
        filename: str,
        file_size: int,
        supported_types: Set[str],
        max_file_size: Optional[int] = None
    ) -> str:
        """Validate file upload parameters.
        
        Args:
            filename: Name of the file
            file_size: Size of the file in bytes
            supported_types: Set of supported file extensions
            max_file_size: Maximum file size in bytes (None for no limit)
            
        Returns:
            The validated file extension
            
        Raises:
            HTTPException: If validation fails
        """
        if not filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename cannot be empty"
            )
        
        # Extract and validate file extension
        parts = filename.rsplit('.', 1)
        if len(parts) != 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename must have an extension"
            )
        
        file_extension = parts[1].lower()
        if file_extension not in supported_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {file_extension}. Supported types: {', '.join(sorted(supported_types))}"
            )
        
        # Validate file size if limit is set
        if max_file_size is not None and file_size > max_file_size:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File size {file_size} bytes exceeds maximum allowed size of {max_file_size} bytes"
            )
        
        return file_extension
    
    @staticmethod
    def validate_mime_type(
        mime_type: str,
        expected_mime_types: Set[str]
    ) -> bool:
        """Validate that a MIME type is in the expected set.
        
        Args:
            mime_type: The MIME type to validate
            expected_mime_types: Set of expected MIME types
            
        Returns:
            True if valid, False otherwise
        """
        return mime_type.lower() in {mt.lower() for mt in expected_mime_types}