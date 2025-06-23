"""Validation utilities for the datasets slice"""
from typing import List, Optional
from fastapi import HTTPException, status


class DatasetValidator:
    """Handles validation logic for datasets"""
    
    SUPPORTED_FILE_TYPES = ['csv', 'xlsx', 'xls', 'xlsm']
    # MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB - File size limit removed
    MAX_TAG_LENGTH = 50
    MAX_TAGS_PER_DATASET = 20
    
    @classmethod
    def validate_file_upload(cls, filename: str, file_size: int) -> None:
        """Validate file before upload"""
        if not filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Filename cannot be empty"
            )
        
        file_extension = filename.split('.')[-1].lower()
        if file_extension not in cls.SUPPORTED_FILE_TYPES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unsupported file type: {file_extension}. Supported types: {', '.join(cls.SUPPORTED_FILE_TYPES)}"
            )
        
        # File size validation removed - no size limit
    
    @classmethod
    def validate_tags(cls, tags: Optional[List[str]]) -> Optional[List[str]]:
        """Validate and clean tags"""
        if not tags:
            return None
        
        if len(tags) > cls.MAX_TAGS_PER_DATASET:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Too many tags. Maximum allowed: {cls.MAX_TAGS_PER_DATASET}"
            )
        
        cleaned_tags = []
        for tag in tags:
            tag = tag.strip()
            if not tag:
                continue
            if len(tag) > cls.MAX_TAG_LENGTH:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Tag '{tag}' exceeds maximum length of {cls.MAX_TAG_LENGTH} characters"
                )
            cleaned_tags.append(tag)
        
        return cleaned_tags if cleaned_tags else None
    
    @classmethod
    def validate_pagination(cls, limit: int, offset: int) -> tuple[int, int]:
        """Validate and normalize pagination parameters"""
        if limit < 1:
            limit = 10
        elif limit > 1000:
            limit = 1000
            
        if offset < 0:
            offset = 0
            
        return limit, offset