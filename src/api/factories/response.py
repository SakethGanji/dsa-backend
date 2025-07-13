"""Factory functions for creating consistent responses."""

from typing import Dict, Any, Type, TypeVar, List, Optional
from pydantic import BaseModel

T = TypeVar('T', bound=BaseModel)


class ResponseFactory:
    """Factory for creating consistent responses."""
    
    @staticmethod
    def from_entity(entity: Dict[str, Any], response_class: Type[T], **overrides) -> T:
        """
        Create response model from entity dict.
        
        Handles common field mappings and allows overrides.
        
        Args:
            entity: Entity dictionary from database
            response_class: Pydantic model class to create
            **overrides: Additional fields or overrides
            
        Returns:
            Instance of response_class
        """
        # Create a copy to avoid modifying original
        data = entity.copy()
        
        # Handle common field mappings
        if 'id' in data:
            # Map id to specific ID fields based on response class fields
            if 'dataset_id' in response_class.__fields__ and 'dataset_id' not in data:
                data['dataset_id'] = data['id']
            elif 'user_id' in response_class.__fields__ and 'user_id' not in data:
                data['user_id'] = data['id']
            elif 'job_id' in response_class.__fields__ and 'job_id' not in data:
                data['job_id'] = str(data['id'])  # Convert UUID to string
        
        # Apply overrides
        data.update(overrides)
        
        return response_class(**data)
    
    @staticmethod
    def create_list_response(
        items: List[Dict[str, Any]],
        total: int,
        offset: int,
        limit: int,
        item_class: Type[T],
        response_class: Optional[Type[BaseModel]] = None,
        **item_overrides
    ) -> Dict[str, Any]:
        """
        Create paginated list response.
        
        Args:
            items: List of entity dictionaries
            total: Total number of items
            offset: Number of items skipped
            limit: Maximum items per page
            item_class: Pydantic model class for items
            response_class: Optional custom response class
            **item_overrides: Overrides applied to each item
            
        Returns:
            Dictionary or response model instance
        """
        # Convert items to response models
        response_items = [
            ResponseFactory.from_entity(item, item_class, **item_overrides) 
            for item in items
        ]
        
        response_data = {
            "items": response_items,
            "total": total,
            "offset": offset,
            "limit": limit,
            "has_more": total > offset + len(items)
        }
        
        if response_class:
            return response_class(**response_data)
        else:
            return response_data
    
    @staticmethod
    def create_operation_response(
        success: bool,
        message: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[Any] = None,
        **additional_fields
    ) -> Dict[str, Any]:
        """
        Create operation response (create, update, delete).
        
        Args:
            success: Whether operation succeeded
            message: Response message
            entity_type: Type of entity operated on
            entity_id: ID of entity operated on
            **additional_fields: Additional response fields
            
        Returns:
            Response dictionary
        """
        response = {
            "success": success,
            "message": message
        }
        
        if entity_type:
            response["entity_type"] = entity_type
        if entity_id is not None:
            response["entity_id"] = entity_id
            
        response.update(additional_fields)
        return response
    
    @staticmethod
    def create_delete_response(
        entity_type: str,
        entity_id: Any,
        custom_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create standardized delete response.
        
        Args:
            entity_type: Type of deleted entity
            entity_id: ID of deleted entity
            custom_message: Optional custom message
            
        Returns:
            Delete response dictionary
        """
        message = custom_message or f"{entity_type} {entity_id} deleted successfully"
        return ResponseFactory.create_operation_response(
            success=True,
            message=message,
            entity_type=entity_type,
            entity_id=entity_id
        )
    
    @staticmethod
    def map_entity_list(
        entities: List[Dict[str, Any]],
        response_class: Type[T],
        **overrides
    ) -> List[T]:
        """
        Map a list of entities to response models.
        
        Args:
            entities: List of entity dictionaries
            response_class: Pydantic model class
            **overrides: Overrides for each entity
            
        Returns:
            List of response model instances
        """
        return [
            ResponseFactory.from_entity(entity, response_class, **overrides)
            for entity in entities
        ]
    
    @staticmethod
    def enrich_with_relations(
        entity: Dict[str, Any],
        relations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Enrich entity with related data.
        
        Args:
            entity: Base entity dictionary
            relations: Related data to merge
            
        Returns:
            Enriched entity dictionary
        """
        enriched = entity.copy()
        enriched.update(relations)
        return enriched