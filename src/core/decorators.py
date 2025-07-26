"""Permission decorators for handler methods."""

from functools import wraps
from typing import Callable, Any
from .domain_exceptions import PermissionDeniedError
import inspect


def requires_permission(resource_type: str, permission_level: str):
    """
    Decorator for handler methods requiring permissions.
    
    This decorator expects the handler to have a unit of work (_uow) attribute
    and will extract user_id and resource_id from the method arguments.
    
    Args:
        resource_type: Type of resource (e.g., "dataset", "job", "file")
        permission_level: Required permission level (e.g., "read", "write", "admin")
    
    Usage:
        @requires_permission("dataset", "write")
        async def update_dataset(self, dataset_id: int, user_id: int, data: dict):
            # Handler logic here
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(self, *args, **kwargs) -> Any:
            # Get function signature to extract parameter names
            sig = inspect.signature(func)
            params = sig.parameters
            
            # Build a mapping of parameter names to values
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()
            arg_dict = bound_args.arguments
            
            # Extract user_id and resource_id
            user_id = None
            resource_id = None
            
            # Try to find user_id
            if 'user_id' in arg_dict:
                user_id = arg_dict['user_id']
            elif 'current_user' in arg_dict:
                user_id = arg_dict['current_user'].user_id
            elif len(args) > 0 and hasattr(args[0], 'user_id'):
                # Check if it's in a command object
                user_id = args[0].user_id
            
            # Try to find resource_id
            # Handle singular/plural resource types
            if resource_type.endswith('s'):
                resource_id_key = f'{resource_type[:-1]}_id'  # datasets -> dataset_id
            else:
                resource_id_key = f'{resource_type}_id'
                
            if resource_id_key in arg_dict:
                resource_id = arg_dict[resource_id_key]
            elif 'id' in arg_dict:
                resource_id = arg_dict['id']
            elif hasattr(args[0], resource_id_key):
                # Check if it's in a command object
                resource_id = getattr(args[0], resource_id_key)
            
            # Validate we have required parameters
            if user_id is None:
                raise ValueError(f"Could not extract user_id from {func.__name__} parameters")
            if resource_id is None:
                raise ValueError(f"Could not extract {resource_id_key} from {func.__name__} parameters")
            
            # Check permission using repository
            if hasattr(self, '_uow'):
                # For dataset permissions, use the existing check_user_permission method
                if resource_type == "dataset" or resource_type == "datasets":
                    has_permission = await self._uow.datasets.check_user_permission(
                        resource_id, user_id, permission_level
                    )
                else:
                    # For other resource types, check if user is admin
                    # A full implementation would check resource-specific permissions
                    user = await self._uow.users.get_by_id(user_id)
                    has_permission = user and user.get('role_name') == 'admin'
                
                if not has_permission:
                    raise PermissionDeniedError(resource_type, permission_level, user_id)
            else:
                raise AttributeError(f"Handler {self.__class__.__name__} does not have _uow attribute")
            
            return await func(self, *args, **kwargs)
        return wrapper
    return decorator


def requires_role(role_name: str):
    """
    Decorator for handler methods requiring specific roles.
    
    Args:
        role_name: Required role name (e.g., "admin", "manager")
    
    Usage:
        @requires_role("admin")
        async def admin_only_operation(self, user_id: int):
            # Handler logic here
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(self, *args, **kwargs) -> Any:
            # Get function signature to extract parameter names
            sig = inspect.signature(func)
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()
            arg_dict = bound_args.arguments
            
            # Extract user
            user = None
            user_id = None
            
            if 'current_user' in arg_dict:
                user = arg_dict['current_user']
            elif 'user_id' in arg_dict and hasattr(self, '_uow'):
                user_id = arg_dict['user_id']
                # Fetch user from repository
                user = await self._uow.users.get_by_id(user_id)
            
            if not user:
                raise ValueError(f"Could not extract user from {func.__name__} parameters")
            
            # Check role
            user_role = user.role_name if hasattr(user, 'role_name') else user.get('role_name')
            
            if role_name == "admin" and user_role != "admin":
                raise PermissionDeniedError("system", "admin", 
                    user.user_id if hasattr(user, 'user_id') else user.get('id'))
            elif role_name == "manager" and user_role not in ["admin", "manager"]:
                raise PermissionDeniedError("system", "manager", 
                    user.user_id if hasattr(user, 'user_id') else user.get('id'))
            
            return await func(self, *args, **kwargs)
        return wrapper
    return decorator